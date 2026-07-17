#!/usr/bin/env python3
"""Overnight SDMX catalog orchestrator: dataset + series shards."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import random
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from parsimony_sdmx.catalog_manifest import (
    DEFAULT_ROOT,
    PHASE_AGENCY_BATCH,
    PHASE_SERIES_FETCH,
    PHASE_SERIES_INDEX,
    STATUS_DEBT,
    STATUS_DONE,
    STATUS_PENDING,
    STATUS_RUNNING,
    BuildRoot,
    Manifest,
    append_debt,
    catalog_shard_done,
    configure_build_logging,
    list_structure_flows,
    write_progress,
)
from parsimony_sdmx.catalog_series import SERIES_AGENCIES
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.core.namespaces import series_namespace

logger = logging.getLogger(__name__)

DEFAULT_AGENCIES = ("ESTAT", "ECB", "IMF_DATA", "WB_WDI")
MAX_ATTEMPTS = 3
BACKOFFS = (30, 120, 300)
FETCH_TIMEOUT_S = 720
INDEX_TIMEOUT_S = 720
HYBRID_BATCH_SIZE = 30


class ThrottleState:
    def __init__(self, concurrency: int) -> None:
        self._lock = threading.Lock()
        self.concurrency = concurrency
        self.initial = concurrency
        self.recent_errors: list[float] = []
        self.last_adjust = time.time()

    def reset(self) -> None:
        """Restore concurrency to its initial value (e.g. between phases).

        Fetch-phase rate-limit errors must not permanently throttle the
        CPU-bound index phase, which talks to no remote API.
        """
        with self._lock:
            self.concurrency = self.initial
            self.recent_errors = []
            self.last_adjust = time.time()

    def record_error(self) -> None:
        with self._lock:
            self.recent_errors.append(time.time())
            cutoff = time.time() - 300
            self.recent_errors = [t for t in self.recent_errors if t >= cutoff]
            if len(self.recent_errors) >= 5 and self.concurrency > 2:
                self.concurrency = max(2, self.concurrency // 2)
                self.last_adjust = time.time()
                logger.warning("Throttle: reduced concurrency to %d", self.concurrency)

    def maybe_recover(self) -> None:
        with self._lock:
            if self.concurrency >= self.initial:
                return
            if time.time() - self.last_adjust > 600 and len(self.recent_errors) < 2:
                self.concurrency = min(self.initial, self.concurrency + 1)
                self.last_adjust = time.time()
                logger.info("Throttle: recovered concurrency to %d", self.concurrency)

    def jitter_sleep(self) -> None:
        time.sleep(random.uniform(0.05, 0.25))


def _disk_usage_gb(path: Path) -> float:
    total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    return round(total / 1e9, 2)


def _worker_script() -> Path:
    return Path(__file__).resolve().parent / "sdmx_catalog_worker.py"


def _build_catalog_script() -> Path:
    return Path(__file__).resolve().parent / "build_catalog.py"


def _run_subprocess(cmd: list[str], *, timeout: float) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        out = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, out.strip()
    except subprocess.TimeoutExpired as exc:
        return 124, f"TimeoutExpired after {timeout}s: {exc}"


def _parse_worker_json(stdout: str) -> dict:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                return parsed
    raise ValueError(f"No JSON payload in worker output: {stdout[:500]}")


def run_agency_batch(
    layout: BuildRoot,
    agency: AgencyId,
    *,
    parallel: int,
    fetch_timeout_s: float,
    max_catalogs: int | None,
) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        str(_build_catalog_script()),
        "--catalog",
        "agency",
        "--agency",
        agency.value,
        "--root",
        str(layout.root),
        "--parallel",
        str(parallel),
        "--fetch-timeout-s",
        str(fetch_timeout_s),
        "--resume",
        "--keep-going",
    ]
    if max_catalogs is not None:
        cmd.extend(["--max-catalogs", str(max_catalogs)])
    code, out = _run_subprocess(cmd, timeout=6 * 3600)
    if code == 0:
        return True, "ok"
    return False, out[-2000:] if out else f"exit {code}"


def run_series_worker(
    layout: BuildRoot,
    agency: AgencyId,
    flow_id: str,
    mode: str,
    *,
    timeout_s: float,
) -> tuple[bool, dict | str]:
    cmd = [
        sys.executable,
        str(_worker_script()),
        "--root",
        str(layout.root),
        "--agency",
        agency.value,
        "--flow",
        flow_id,
        "--mode",
        mode,
    ]
    code, out = _run_subprocess(cmd, timeout=timeout_s)
    if code == 124:
        return False, "timeout"
    try:
        payload = _parse_worker_json(out)
    except (json.JSONDecodeError, ValueError):
        return False, out[-2000:] if out else f"exit {code}"
    if not payload.get("ok"):
        return False, str(payload.get("error", "worker failed"))
    return True, payload


def run_series_index_batch_worker(
    layout: BuildRoot,
    agency: AgencyId,
    flow_ids: list[str],
    *,
    timeout_s: float,
) -> dict[str, tuple[bool, dict | str]]:
    """Index a batch of small flows in one subprocess (shared embedder).

    Returns a per-flow {flow_id: (ok, payload|error)} map. A batch-fatal failure
    (timeout, OOM, unparseable output) is reported against every flow so the
    orchestrator can retry/debt them individually.
    """
    flows_file = layout.staging / "index_batches" / f"{agency.value}_{abs(hash(tuple(flow_ids)))}.txt"
    flows_file.parent.mkdir(parents=True, exist_ok=True)
    flows_file.write_text("\n".join(flow_ids), encoding="utf-8")
    cmd = [
        sys.executable,
        str(_worker_script()),
        "--root",
        str(layout.root),
        "--agency",
        agency.value,
        "--flows-file",
        str(flows_file),
        "--mode",
        "series-index-batch",
    ]
    code, out = _run_subprocess(cmd, timeout=timeout_s)
    with contextlib.suppress(OSError):
        flows_file.unlink()

    def _all(reason: str) -> dict[str, tuple[bool, dict | str]]:
        return {fid: (False, reason) for fid in flow_ids}

    if code == 124:
        return _all("timeout")
    try:
        payload = _parse_worker_json(out)
    except (json.JSONDecodeError, ValueError):
        return _all(out[-2000:] if out else f"exit {code}")
    if not payload.get("ok"):
        return _all(str(payload.get("error", "batch worker failed")))

    results: dict[str, tuple[bool, dict | str]] = {}
    for item in payload.get("results", []):
        fid = item.get("flow_id")
        if fid is None:
            continue
        if item.get("ok"):
            results[fid] = (True, item)
        else:
            results[fid] = (False, str(item.get("error", "flow failed")))
    for fid in flow_ids:
        results.setdefault(fid, (False, "missing from batch output"))
    return results


def _fetch_done(layout: BuildRoot, namespace: str) -> bool:
    meta = layout.staging / "series" / namespace / "fetch_meta.json"
    parquet = layout.staging / "series" / namespace / "series.parquet"
    return meta.is_file() and parquet.is_file()


def _should_retry(error: str) -> bool:
    transient = (
        "timeout",
        "Timeout",
        "429",
        "503",
        "502",
        "504",
        "Connection",
        "ConnectionError",
        "RemoteDisconnected",
        "MemoryError",
    )
    return any(tok in error for tok in transient)


def _handle_task_failure(
    manifest: Manifest,
    layout: BuildRoot,
    *,
    agency: str,
    phase: str,
    flow_id: str,
    error: str,
) -> None:
    task = manifest.get_task(agency, phase, flow_id)
    attempts = (task["attempts"] if task else 0) + 1
    if attempts >= MAX_ATTEMPTS or not _should_retry(error):
        manifest.upsert_task(
            agency=agency,
            phase=phase,
            flow_id=flow_id,
            status=STATUS_DEBT,
            error=error[:2000],
            increment_attempts=True,
        )
        append_debt(layout.debt_log, agency=agency, phase=phase, flow_id=flow_id, reason=error)
        logger.error("DEBT %s/%s/%s: %s", agency, phase, flow_id or "-", error[:200])
    else:
        manifest.upsert_task(
            agency=agency,
            phase=phase,
            flow_id=flow_id,
            status=STATUS_PENDING,
            error=error[:2000],
            increment_attempts=True,
        )
        wait = BACKOFFS[min(attempts - 1, len(BACKOFFS) - 1)]
        logger.warning("Retry scheduled in %ds for %s/%s/%s (attempt %d)", wait, agency, phase, flow_id, attempts)
        time.sleep(wait + random.uniform(0, 5))


def _execute_series_phase(
    layout: BuildRoot,
    manifest: Manifest,
    throttle: ThrottleState,
    *,
    agency: AgencyId,
    phase: str,
    mode: str,
    flows: list[str],
    concurrency: int,
    timeout_s: float,
    resume: bool,
) -> None:
    pending: list[str] = []
    for flow_id in flows:
        ns = series_namespace(agency, flow_id)
        task = manifest.get_task(agency.value, phase, flow_id)
        if task and task["status"] == STATUS_DONE:
            continue
        if resume:
            if phase == PHASE_SERIES_FETCH and _fetch_done(layout, ns):
                count = json.loads((layout.staging / "series" / ns / "fetch_meta.json").read_text())["series_count"]
                manifest.upsert_task(
                    agency=agency.value,
                    phase=phase,
                    flow_id=flow_id,
                    status=STATUS_DONE,
                    series_count=count,
                )
                continue
            if phase == PHASE_SERIES_INDEX and catalog_shard_done(layout.catalogs, ns):
                manifest.upsert_task(agency=agency.value, phase=phase, flow_id=flow_id, status=STATUS_DONE)
                continue
        if task and task["status"] == STATUS_DEBT:
            continue
        if phase == PHASE_SERIES_INDEX:
            fetch_task = manifest.get_task(agency.value, PHASE_SERIES_FETCH, flow_id)
            if not fetch_task or fetch_task["status"] != STATUS_DONE:
                continue
        pending.append(flow_id)

    if not pending:
        return

    def _record(flow_id: str, ok: bool, payload: dict | str) -> None:
        if ok:
            series_count = payload.get("series_count") if isinstance(payload, dict) else None
            manifest.upsert_task(
                agency=agency.value,
                phase=phase,
                flow_id=flow_id,
                status=STATUS_DONE,
                series_count=series_count,
            )
            logger.info("DONE %s %s %s count=%s", agency.value, phase, flow_id, series_count)
        else:
            err = payload if isinstance(payload, str) else json.dumps(payload)
            throttle.record_error()
            _handle_task_failure(
                manifest,
                layout,
                agency=agency.value,
                phase=phase,
                flow_id=flow_id,
                error=err,
            )
        throttle.maybe_recover()

    def _run_batch(batch: list[str], workers: int) -> None:
        if not batch:
            return
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {}
            for flow_id in batch:
                manifest.upsert_task(agency=agency.value, phase=phase, flow_id=flow_id, status=STATUS_RUNNING)

                def _job(fid: str = flow_id) -> tuple[str, bool, dict | str]:
                    throttle.jitter_sleep()
                    ok, payload = run_series_worker(
                        layout,
                        agency,
                        fid,
                        mode,
                        timeout_s=timeout_s,
                    )
                    return fid, ok, payload

                futures[pool.submit(_job)] = flow_id

            for fut in as_completed(futures):
                flow_id, ok, payload = fut.result()
                _record(flow_id, ok, payload)

    if phase != PHASE_SERIES_INDEX:
        _run_batch(pending, concurrency)
        return

    workers = max(1, concurrency)
    logger.info("Building columnar series stores for %s: %d flows (batched)", agency.value, len(pending))
    groups = [pending[i : i + HYBRID_BATCH_SIZE] for i in range(0, len(pending), HYBRID_BATCH_SIZE)]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for group in groups:
            for fid in group:
                manifest.upsert_task(agency=agency.value, phase=phase, flow_id=fid, status=STATUS_RUNNING)
            batch_timeout = 60.0 + len(group) * 45.0

            def _job(g: list[str] = group, t: float = batch_timeout) -> dict[str, tuple[bool, dict | str]]:
                throttle.jitter_sleep()
                return run_series_index_batch_worker(layout, agency, g, timeout_s=t)

            futures[pool.submit(_job)] = group

        for fut in as_completed(futures):
            for flow_id, (ok, payload) in fut.result().items():
                _record(flow_id, ok, payload)


def run_build(
    layout: BuildRoot,
    manifest: Manifest,
    *,
    agencies: list[AgencyId],
    concurrency: int,
    structure_parallel: int,
    max_catalogs: int | None,
    resume: bool,
) -> None:
    throttle = ThrottleState(concurrency)
    start = time.time()

    for agency in agencies:
        logger.info("=== Agency %s: batch (structure + datasets) ===", agency.value)
        batch_task = manifest.get_task(agency.value, PHASE_AGENCY_BATCH, "")
        if not (resume and batch_task and batch_task["status"] == STATUS_DONE):
            manifest.upsert_task(agency=agency.value, phase=PHASE_AGENCY_BATCH, flow_id="", status=STATUS_RUNNING)
            ok, err = run_agency_batch(
                layout,
                agency,
                parallel=structure_parallel,
                fetch_timeout_s=120.0,
                max_catalogs=max_catalogs,
            )
            if ok:
                manifest.upsert_task(agency=agency.value, phase=PHASE_AGENCY_BATCH, flow_id="", status=STATUS_DONE)
            else:
                _handle_task_failure(
                    manifest,
                    layout,
                    agency=agency.value,
                    phase=PHASE_AGENCY_BATCH,
                    flow_id="",
                    error=err,
                )
                continue

        if agency not in SERIES_AGENCIES:
            logger.info("Skipping series phases for %s (no CSV series support)", agency.value)
            continue

        flows = list_structure_flows(layout.structures, agency.value)
        if max_catalogs is not None:
            flows = flows[:max_catalogs]
        manifest.ensure_series_tasks(agency.value, flows)
        logger.info("=== Agency %s: series-fetch (%d flows) ===", agency.value, len(flows))
        _execute_series_phase(
            layout,
            manifest,
            throttle,
            agency=agency,
            phase=PHASE_SERIES_FETCH,
            mode="series-fetch",
            flows=flows,
            concurrency=throttle.concurrency,
            timeout_s=FETCH_TIMEOUT_S,
            resume=resume,
        )
        throttle.reset()
        logger.info("=== Agency %s: series-index (throttle reset to %d) ===", agency.value, throttle.concurrency)
        _execute_series_phase(
            layout,
            manifest,
            throttle,
            agency=agency,
            phase=PHASE_SERIES_INDEX,
            mode="series-index",
            flows=flows,
            concurrency=throttle.concurrency,
            timeout_s=INDEX_TIMEOUT_S,
            resume=resume,
        )

    elapsed = time.time() - start
    logger.info("Build loop finished in %.0fs", elapsed)


def print_status(layout: BuildRoot, manifest: Manifest) -> None:
    print(f"Root: {layout.root}")
    print(f"Disk catalogs: {_disk_usage_gb(layout.catalogs)} GB")
    print(f"Disk total: {_disk_usage_gb(layout.root)} GB")
    if layout.progress_json.is_file():
        print("\nLast heartbeat:")
        print(layout.progress_json.read_text(encoding="utf-8")[:2000])
    print("\nTask summary:")
    for agency in DEFAULT_AGENCIES:
        for phase in (PHASE_AGENCY_BATCH, PHASE_SERIES_FETCH, PHASE_SERIES_INDEX):
            counts = manifest.count_by_status(agency=agency, phase=phase)
            if counts:
                print(f"  {agency}/{phase}: {counts}")
    debt_lines = 0
    if layout.debt_log.is_file():
        debt_lines = sum(1 for _ in layout.debt_log.open())
    print(f"\nDebt entries: {debt_lines}")


def heartbeat_loop(layout: BuildRoot, db_path: Path, stop: threading.Event, interval: float = 30.0) -> None:
    conn = sqlite3.connect(str(db_path), timeout=60.0, isolation_level=None)
    try:
        while not stop.wait(interval):
            payload: dict = {"ts": time.time(), "disk_gb": _disk_usage_gb(layout.root), "agencies": {}}
            for agency in DEFAULT_AGENCIES:
                counts: dict[str, dict[str, int]] = {}
                for phase in (PHASE_AGENCY_BATCH, PHASE_SERIES_FETCH, PHASE_SERIES_INDEX):
                    cur = conn.execute(
                        "SELECT status, COUNT(*) FROM tasks WHERE agency=? AND phase=? GROUP BY status",
                        (agency, phase),
                    )
                    counts[phase] = {str(status): int(count) for status, count in cur.fetchall()}
                payload["agencies"][agency] = counts
            write_progress(layout.progress_json, payload)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument(
        "--agencies",
        default=",".join(DEFAULT_AGENCIES),
        help="Comma-separated agency ids",
    )
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--structure-parallel", type=int, default=6)
    parser.add_argument("--max-catalogs", type=int, default=None, help="Limit flows per agency (dry-run)")
    parser.add_argument("--retry-debt", action="store_true", help="Reset debt tasks to pending before run")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()

    layout = BuildRoot.create(args.root)
    configure_build_logging(layout.build_log)
    manifest = Manifest(layout.manifest_db)
    try:
        if args.status:
            print_status(layout, manifest)
            return

        if args.retry_debt:
            n = manifest.reset_debt()
            logger.info("Reset %d debt tasks to pending", n)

        if not args.no_resume:
            reset = manifest.reset_stale_running()
            if reset:
                logger.info("Reset %d stale running tasks to pending", reset)

        agencies = [AgencyId(a.strip()) for a in args.agencies.split(",") if a.strip()]
        stop = threading.Event()
        hb = threading.Thread(target=heartbeat_loop, args=(layout, layout.manifest_db, stop), daemon=True)
        hb.start()
        try:
            run_build(
                layout,
                manifest,
                agencies=agencies,
                concurrency=args.concurrency,
                structure_parallel=args.structure_parallel,
                max_catalogs=args.max_catalogs,
                resume=not args.no_resume,
            )
        finally:
            stop.set()
            hb.join(timeout=2)
        print_status(layout, manifest)
    finally:
        manifest.close()


if __name__ == "__main__":
    main()
