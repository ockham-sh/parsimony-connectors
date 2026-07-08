"""Ledger-driven release catalog build orchestrator.

DEPRECATED: use ``packages/sdmx/scripts/build_all_catalogs.py`` for initial release.
This script remains for structure-marker sweeps only; it does not emit codelists or
series shards required for end-to-end SDMX search.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import resource
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from parsimony_sdmx._isolation import LIST_DEFAULT_TIMEOUT_S, ListDatasetsError, list_datasets
from parsimony_sdmx.core.agencies import ALL_AGENCIES, AgencyId
from parsimony_sdmx.core.models import DatasetRecord


# Import structure marker helper from build script path at runtime in child only;
# duplicate minimal namespace helper here to avoid importing build_catalog in parent.
def structure_marker_namespace(agency: AgencyId, dataset_id: str) -> str:
    return f"sdmx_structure_{agency.value.lower()}_{dataset_id.lower()}"


logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = SCRIPT_DIR.parent
DEFAULT_SAVE_ROOT = "/tmp/parsimony-catalogs-v1/sdmx"
DEFAULT_STATUS_DIR = "/tmp/parsimony-catalog-build"

FETCH_TIMEOUTS_S: tuple[float, ...] = (120.0, 300.0)
MIN_AVAIL_KB = 1_500_000
RLIMIT_AS_BYTES = 6 * 1024**3
INDEX_BUFFER_S = 120.0

TERMINAL_STATES = frozenset({"built", "lazy", "skipped"})

# Substrings that mark a failure as a deterministic size/timeout problem rather
# than a transient one. A flow that can't fetch its series keys inside the
# attempt-1 budget is too large for live enumeration; a longer retry just burns
# wall time before failing again, so we send it straight to ``lazy`` (runtime
# lazy-build still serves it on first search). Transient failures (HTTP 5xx,
# resets, malformed payloads) don't match and keep the escalated retry.
_TIMEOUT_FAILURE_MARKERS: tuple[str, ...] = ("exceeded timeout of", "wall-clock timeout")


def _is_timeout_failure(error: str | None) -> bool:
    if not error:
        return False
    lowered = error.lower()
    return any(marker in lowered for marker in _TIMEOUT_FAILURE_MARKERS)


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    namespace: str
    dataset_id: str
    state: str
    attempt: int
    rows: int | None = None
    seconds: float | None = None
    error: str | None = None
    ts: str = ""

    def to_json(self) -> str:
        payload = asdict(self)
        if not payload["ts"]:
            payload["ts"] = datetime.now(UTC).isoformat()
        return json.dumps(payload, separators=(",", ":"))


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ledger_path(save_root: Path, agency: AgencyId) -> Path:
    return save_root / f"build_ledger_{agency.value.lower()}.jsonl"


_LEDGER_LOCK = threading.Lock()


def append_event(path: Path, event: LedgerEvent) -> None:
    """Append one event durably.

    Serialized across threads and fsync'd so a crash mid-write can't leave a
    torn line, and ext4 delayed allocation can't surface a null-byte tail that
    a later resume would choke on.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (event.to_json() + "\n").encode("utf-8")
    with _LEDGER_LOCK:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)


def load_ledger(path: Path) -> dict[str, LedgerEvent]:
    """Return the latest event per namespace, tolerating a corrupt tail.

    Any unparseable line (null bytes from an interrupted write, a truncated
    final record) is skipped rather than aborting the whole resume. The
    append-only design means at most the last few records are ever at risk.
    """
    if not path.is_file():
        return {}
    latest: dict[str, LedgerEvent] = {}
    skipped = 0
    with path.open("rb") as handle:
        for raw_line in handle:
            line = raw_line.replace(b"\x00", b"").strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
                event = LedgerEvent(
                    namespace=raw["namespace"],
                    dataset_id=raw["dataset_id"],
                    state=raw["state"],
                    attempt=int(raw["attempt"]),
                    rows=raw.get("rows"),
                    seconds=raw.get("seconds"),
                    error=raw.get("error"),
                    ts=raw.get("ts", ""),
                )
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                skipped += 1
                continue
            latest[event.namespace] = event
    if skipped:
        logger.warning("Skipped %d corrupt ledger line(s) in %s", skipped, path.name)
    return latest


def snapshot_exists(save_root: Path, namespace: str) -> bool:
    return (save_root / namespace / "meta.json").is_file()


def mem_available_kb() -> int:
    try:
        with Path("/proc/meminfo").open(encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def network_up() -> bool:
    """DNS probe — distinguishes upstream flow failures from local outages."""
    import socket

    try:
        socket.getaddrinfo("ec.europa.eu", 443)
        return True
    except OSError:
        return False


def _wait_for_network() -> None:
    while not network_up():
        logger.warning("Network/DNS down; sleeping 60s before next flow")
        time.sleep(60)


def count_structures_on_disk(save_root: Path, agency: AgencyId) -> int:
    prefix = f"sdmx_structure_{agency.value.lower()}_"
    if not save_root.is_dir():
        return 0
    return sum(
        1
        for sub in save_root.iterdir()
        if sub.is_dir() and sub.name.startswith(prefix) and (sub / "meta.json").is_file()
    )


def selected_flows(
    agency: AgencyId,
    *,
    skip_ids: set[str],
) -> list[DatasetRecord]:
    if agency is AgencyId.WB_WDI:
        return [DatasetRecord(dataset_id="WDI", agency_id=agency.value, title="World Development Indicators")]
    try:
        records = list_datasets(agency.value, LIST_DEFAULT_TIMEOUT_S)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {agency.value}: {exc.message}") from exc
    return [r for r in records if "$" not in r.dataset_id and r.dataset_id.upper() not in skip_ids]


def seed_ledger(
    save_root: Path,
    agency: AgencyId,
    *,
    skip_ids: set[str],
) -> dict[str, LedgerEvent]:
    path = ledger_path(save_root, agency)
    existing = load_ledger(path)
    if existing:
        return existing

    flows = selected_flows(agency, skip_ids=skip_ids)
    state: dict[str, LedgerEvent] = {}
    for record in flows:
        namespace = structure_marker_namespace(agency, record.dataset_id)
        ts = _now_iso()
        if record.dataset_id.upper() in skip_ids:
            event = LedgerEvent(namespace=namespace, dataset_id=record.dataset_id, state="skipped", attempt=0, ts=ts)
        elif snapshot_exists(save_root, namespace):
            event = LedgerEvent(namespace=namespace, dataset_id=record.dataset_id, state="built", attempt=0, ts=ts)
        else:
            event = LedgerEvent(namespace=namespace, dataset_id=record.dataset_id, state="pending", attempt=0, ts=ts)
        append_event(path, event)
        state[namespace] = event
    built_on_disk = sum(1 for e in state.values() if e.state == "built")
    logger.info("Seeded %s ledger: %d flows (%d built on disk)", agency.value, len(state), built_on_disk)
    return state


def _child_preexec() -> None:
    os.setsid()
    resource.setrlimit(resource.RLIMIT_AS, (RLIMIT_AS_BYTES, RLIMIT_AS_BYTES))


def _wall_timeout_s(fetch_timeout_s: float) -> float:
    return fetch_timeout_s + INDEX_BUFFER_S


def _parse_child_result(stdout: str) -> tuple[int | None, str | None]:
    rows: int | None = None
    error: str | None = None
    for line in stdout.splitlines():
        if "Built " in line and " catalog with " in line and " entries" in line:
            with contextlib.suppress(ValueError):
                rows = int(line.split(" catalog with ", 1)[1].split(" entries", 1)[0])
        if line.startswith("ERROR ") or "FAILED " in line:
            error = line.strip()
    return rows, error


def run_flow_child(
    *,
    python: str,
    save_root: Path,
    agency: AgencyId,
    dataset_id: str,
    fetch_timeout_s: float,
    log_dir: Path,
) -> tuple[bool, int | None, float, str | None]:
    namespace = structure_marker_namespace(agency, dataset_id)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{namespace}.log"
    cmd = [
        python,
        str(SCRIPT_DIR / "build_catalog.py"),
        "--catalog",
        "structure",
        "--agency",
        agency.value,
        "--dataset-id",
        dataset_id,
        "--save-root",
        str(save_root),
        "--fetch-timeout-s",
        str(fetch_timeout_s),
    ]
    env = os.environ.copy()
    env.setdefault("HF_HUB_OFFLINE", "1")
    env.setdefault("TRANSFORMERS_OFFLINE", "1")
    start = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_wall_timeout_s(fetch_timeout_s),
            preexec_fn=_child_preexec,
            env=env,
            cwd=PACKAGE_ROOT,
        )
    except subprocess.TimeoutExpired:
        wall_s = _wall_timeout_s(fetch_timeout_s)
        return False, None, time.monotonic() - start, f"wall-clock timeout after {wall_s:.0f}s"
    elapsed = time.monotonic() - start
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    log_file.write_text(combined, encoding="utf-8")
    rows, parsed_error = _parse_child_result(combined)
    if proc.returncode == 0 and snapshot_exists(save_root, namespace):
        return True, rows, elapsed, None
    err = parsed_error or (proc.stderr or "").strip().splitlines()[-1] if proc.stderr else f"exit {proc.returncode}"
    return False, rows, elapsed, err


def next_attempt(event: LedgerEvent) -> int:
    if event.state in TERMINAL_STATES | {"running"}:
        return max(1, event.attempt)
    return event.attempt + 1 if event.attempt > 0 else 1


def fetch_timeout_for_attempt(attempt: int, agency: AgencyId) -> float | None:
    if attempt <= 0:
        return FETCH_TIMEOUTS_S[0]
    if attempt <= len(FETCH_TIMEOUTS_S):
        return FETCH_TIMEOUTS_S[attempt - 1]
    return None


def process_one_flow(
    *,
    python: str,
    save_root: Path,
    agency: AgencyId,
    ledger_file: Path,
    event: LedgerEvent,
    log_dir: Path,
) -> LedgerEvent:
    if event.state in TERMINAL_STATES:
        return event

    attempt = next_attempt(event)
    timeout_s = fetch_timeout_for_attempt(attempt, agency)
    if timeout_s is None:
        lazy = LedgerEvent(
            namespace=event.namespace,
            dataset_id=event.dataset_id,
            state="lazy",
            attempt=attempt,
            error=event.error or "max attempts exhausted",
            ts=_now_iso(),
        )
        append_event(ledger_file, lazy)
        return lazy

    running = LedgerEvent(
        namespace=event.namespace,
        dataset_id=event.dataset_id,
        state="running",
        attempt=attempt,
        ts=_now_iso(),
    )
    append_event(ledger_file, running)

    ok, rows, seconds, error = run_flow_child(
        python=python,
        save_root=save_root,
        agency=agency,
        dataset_id=event.dataset_id,
        fetch_timeout_s=timeout_s,
        log_dir=log_dir,
    )
    if ok:
        built = LedgerEvent(
            namespace=event.namespace,
            dataset_id=event.dataset_id,
            state="built",
            attempt=attempt,
            rows=rows,
            seconds=seconds,
            ts=_now_iso(),
        )
        append_event(ledger_file, built)
        logger.info("BUILT %s (%s rows, %.0fs)", event.namespace, rows, seconds or 0.0)
        return built

    if not network_up():
        # Local outage, not a flow problem: requeue without burning the attempt.
        requeued = LedgerEvent(
            namespace=event.namespace,
            dataset_id=event.dataset_id,
            state="pending",
            attempt=attempt - 1,
            error=error,
            ts=_now_iso(),
        )
        append_event(ledger_file, requeued)
        logger.warning(
            "TRANSIENT %s attempt %d failed during network outage — requeued: %s",
            event.namespace,
            attempt,
            error,
        )
        return requeued

    timed_out = _is_timeout_failure(error)
    if attempt >= len(FETCH_TIMEOUTS_S) or timed_out:
        final = LedgerEvent(
            namespace=event.namespace,
            dataset_id=event.dataset_id,
            state="lazy",
            attempt=attempt,
            seconds=seconds,
            error=error,
            ts=_now_iso(),
        )
        append_event(ledger_file, final)
        reason = "fetch-keys timeout (no long retry)" if timed_out else f"{attempt} attempt(s)"
        logger.warning("LAZY %s after %s: %s", event.namespace, reason, error)
        return final

    pending = LedgerEvent(
        namespace=event.namespace,
        dataset_id=event.dataset_id,
        state="pending",
        attempt=attempt,
        seconds=seconds,
        error=error,
        ts=_now_iso(),
    )
    append_event(ledger_file, pending)
    logger.warning("RETRY %s attempt %d failed: %s", event.namespace, attempt, error)
    return pending


def write_status_file(
    *,
    status_dir: Path,
    agency: AgencyId | None,
    phase: str,
    note: str,
    ledger_states: dict[str, LedgerEvent],
    save_root: Path,
) -> None:
    status_dir.mkdir(parents=True, exist_ok=True)
    status_file = status_dir / "status.txt"
    tmp = status_file.with_suffix(".tmp")
    counts: dict[str, int] = {}
    for event in ledger_states.values():
        counts[event.state] = counts.get(event.state, 0) + 1

    def _agency_line(a: AgencyId) -> str:
        return f"{count_structures_on_disk(save_root, a)}"

    lines = [
        f"updated={_now_iso()}",
        f"phase={phase}",
        f"note={note}",
        "orchestrator=build_release.py",
        f"agency={agency.value if agency else 'all'}",
        f"pending={counts.get('pending', 0)}",
        f"running={counts.get('running', 0)}",
        f"built={counts.get('built', 0)}",
        f"lazy={counts.get('lazy', 0)}",
        f"skipped={counts.get('skipped', 0)}",
        f"ecb_structures={_agency_line(AgencyId.ECB)}",
        f"imf_structures={_agency_line(AgencyId.IMF_DATA)}",
        f"wb_structures={_agency_line(AgencyId.WB_WDI)}",
        f"estat_structures={_agency_line(AgencyId.ESTAT)}",
        f"mem_avail_mb={mem_available_kb() // 1024}",
    ]
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    tmp.replace(status_file)


def summarize_ledger(states: dict[str, LedgerEvent]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in states.values():
        counts[event.state] = counts.get(event.state, 0) + 1
    return counts


def cmd_status(args: argparse.Namespace) -> None:
    save_root = Path(args.save_root)
    agencies = [args.agency] if args.agency else list(ALL_AGENCIES)
    for agency in agencies:
        states = load_ledger(ledger_path(save_root, agency))
        if not states:
            states = seed_ledger(save_root, agency, skip_ids=_skip_ids(args))
        counts = summarize_ledger(states)
        disk = count_structures_on_disk(save_root, agency)
        print(
            f"{agency.value}: disk={disk} pending={counts.get('pending', 0)} "
            f"built={counts.get('built', 0)} lazy={counts.get('lazy', 0)} "
            f"skipped={counts.get('skipped', 0)}"
        )
    if args.status_dir:
        write_status_file(
            status_dir=Path(args.status_dir),
            agency=args.agency,
            phase="status",
            note="status command",
            ledger_states=states if args.agency else {},
            save_root=save_root,
        )


def _skip_ids(args: argparse.Namespace) -> set[str]:
    return {item.strip().upper() for item in args.skip_dataset_id or [] if item.strip()}


def _wait_for_memory(min_kb: int) -> None:
    while mem_available_kb() < min_kb:
        avail = mem_available_kb()
        logger.info("Waiting for memory: %d MB available (need %d MB)", avail // 1024, min_kb // 1024)
        time.sleep(30)


def _normalize_running_states(ledger_file: Path, states: dict[str, LedgerEvent]) -> None:
    """Crash recovery: ``running`` rows from a dead parent become ``pending``."""
    for event in states.values():
        if event.state == "running":
            pending = LedgerEvent(
                namespace=event.namespace,
                dataset_id=event.dataset_id,
                state="pending",
                attempt=event.attempt,
                error=event.error,
                ts=_now_iso(),
            )
            append_event(ledger_file, pending)


def run_agency(
    *,
    python: str,
    save_root: Path,
    agency: AgencyId,
    parallel: int,
    skip_ids: set[str],
    status_dir: Path,
    log_dir: Path,
) -> dict[str, LedgerEvent]:
    ledger_file = ledger_path(save_root, agency)
    states = seed_ledger(save_root, agency, skip_ids=skip_ids)
    _normalize_running_states(ledger_file, states)
    states = load_ledger(ledger_file)

    pending = [e for e in states.values() if e.state not in TERMINAL_STATES]
    if not pending:
        logger.info("%s: nothing pending", agency.value)
        return load_ledger(ledger_file)

    write_status_file(
        status_dir=status_dir,
        agency=agency,
        phase=agency.value.lower(),
        note=f"starting {len(pending)} pending flows",
        ledger_states=states,
        save_root=save_root,
    )

    with ThreadPoolExecutor(max_workers=max(1, parallel)) as pool:
        in_flight: dict[Future[LedgerEvent], str] = {}
        while True:
            states = load_ledger(ledger_file)
            active = set(in_flight.values())
            pending = [e for e in states.values() if e.state not in TERMINAL_STATES and e.namespace not in active]
            if not pending and not in_flight:
                break

            while pending and len(in_flight) < parallel:
                _wait_for_network()
                _wait_for_memory(MIN_AVAIL_KB)
                event = pending.pop(0)
                if event.state in TERMINAL_STATES or event.namespace in in_flight.values():
                    continue
                fut = pool.submit(
                    process_one_flow,
                    python=python,
                    save_root=save_root,
                    agency=agency,
                    ledger_file=ledger_file,
                    event=event,
                    log_dir=log_dir,
                )
                in_flight[fut] = event.namespace

            if not in_flight:
                time.sleep(5)
                continue

            done, _ = wait(in_flight.keys(), timeout=30, return_when=FIRST_COMPLETED)
            for fut in done:
                namespace = in_flight.pop(fut)
                try:
                    fut.result()
                except Exception:
                    logger.exception("Worker failed for %s", namespace)
            states = load_ledger(ledger_file)
            write_status_file(
                status_dir=status_dir,
                agency=agency,
                phase=agency.value.lower(),
                note=f"in_flight={len(in_flight)}",
                ledger_states=states,
                save_root=save_root,
            )

    return load_ledger(ledger_file)


def cmd_run(args: argparse.Namespace) -> int:
    save_root = Path(args.save_root)
    save_root.mkdir(parents=True, exist_ok=True)
    python = args.python or sys.executable
    status_dir = Path(args.status_dir)
    log_dir = status_dir / "release-logs"
    skip_ids = _skip_ids(args)

    agencies = [args.agency] if args.agency else list(ALL_AGENCIES)
    exit_code = 0
    for agency in agencies:
        logger.info("=== %s structure sweep ===", agency.value)
        final = run_agency(
            python=python,
            save_root=save_root,
            agency=agency,
            parallel=args.parallel,
            skip_ids=skip_ids,
            status_dir=status_dir,
            log_dir=log_dir,
        )
        counts = summarize_ledger(final)
        if counts.get("pending", 0) or counts.get("running", 0):
            exit_code = 1
        if args.enrich_after and counts.get("pending", 0) == 0 and counts.get("running", 0) == 0:
            # Enrichment is secondary to series generation; a corrupt/transient
            # datasets-catalog failure must never abort the sweep. Log and move on.
            try:
                cmd_enrich(argparse.Namespace(**{**vars(args), "agency": agency}))
            except Exception:
                logger.exception("Enrich failed for %s; continuing (re-run enrich later)", agency.value)

    write_status_file(
        status_dir=status_dir,
        agency=args.agency,
        phase="done",
        note="run complete",
        ledger_states=final if args.agency else {},
        save_root=save_root,
    )
    completion = status_dir / "completion.txt"
    completion.write_text(
        "\n".join(
            [
                f"finished={_now_iso()}",
                "orchestrator=build_release.py",
                f"ecb_structures={count_structures_on_disk(save_root, AgencyId.ECB)}",
                f"imf_structures={count_structures_on_disk(save_root, AgencyId.IMF_DATA)}",
                f"estat_structures={count_structures_on_disk(save_root, AgencyId.ESTAT)}",
                f"wb_structures={count_structures_on_disk(save_root, AgencyId.WB_WDI)}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return exit_code


def cmd_enrich(args: argparse.Namespace) -> None:
    python = args.python or sys.executable
    cmd = [
        python,
        str(SCRIPT_DIR / "build_catalog.py"),
        "--catalog",
        "datasets",
        "--agency",
        args.agency.value,
        "--save-root",
        str(args.save_root),
    ]
    logger.info("Enriching datasets catalog for %s", args.agency.value)
    subprocess.run(cmd, check=True, cwd=PACKAGE_ROOT)


def cmd_validate(args: argparse.Namespace) -> int:
    save_root = Path(args.save_root)
    issues: list[str] = []
    for agency in [args.agency] if args.agency else list(ALL_AGENCIES):
        states = load_ledger(ledger_path(save_root, agency))
        if not states:
            states = seed_ledger(save_root, agency, skip_ids=_skip_ids(args))
        for event in states.values():
            if event.state == "built" and not snapshot_exists(save_root, event.namespace):
                issues.append(f"{event.namespace}: ledger=built but meta.json missing")
        ds_path = save_root / f"sdmx_datasets_{agency.value.lower()}"
        if not (ds_path / "meta.json").is_file():
            issues.append(f"{ds_path.name}: datasets catalog missing (run enrich)")
        lazy = [e.dataset_id for e in states.values() if e.state == "lazy"]
        if lazy:
            logger.info("%s lazy flows (%d): %s", agency.value, len(lazy), ", ".join(lazy[:10]))
    if issues:
        for issue in issues:
            logger.error("VALIDATE: %s", issue)
        return 1
    logger.info("Validation OK")
    return 0


def _add_common_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--save-root", default=DEFAULT_SAVE_ROOT)
    parser.add_argument("--status-dir", default=DEFAULT_STATUS_DIR)
    parser.add_argument("--python", help="Python executable for child builds")
    parser.add_argument("--agency", type=AgencyId)
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--skip-dataset-id", action="append", default=[])


def main() -> None:
    import warnings

    warnings.warn(
        "build_release.py is deprecated; use packages/sdmx/scripts/build_all_catalogs.py for initial release.",
        DeprecationWarning,
        stacklevel=1,
    )
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    status_parser = sub.add_parser("status")
    _add_common_flags(status_parser)

    run_parser = sub.add_parser("run")
    _add_common_flags(run_parser)
    run_parser.add_argument("--enrich-after", action="store_true")

    enrich_parser = sub.add_parser("enrich")
    _add_common_flags(enrich_parser)

    validate_parser = sub.add_parser("validate")
    _add_common_flags(validate_parser)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.command == "status":
        cmd_status(args)
        return
    if args.command == "enrich":
        if args.agency is None:
            parser.error("--agency is required for enrich")
        cmd_enrich(args)
        return
    if args.command == "validate":
        raise SystemExit(cmd_validate(args))
    raise SystemExit(cmd_run(args))


if __name__ == "__main__":
    main()
