"""Run datasets in isolated subprocesses with filesystem-backed resume.

Each dataset runs in a fresh ``multiprocessing.Process`` so peak memory
is bounded and a crashing worker cannot corrupt the parent. The memory
monitor thread watches system RSS in the parent and ``SIGKILL``s the
largest child when it crosses the threshold, writing an OOM marker so
the reaped exit can be classified as ``FailureKind.OOM_KILLED``.

Resume works from the filesystem alone: atomic ``.tmp/ → os.replace()``
means the canonical series parquet exists iff the write completed, so
``--force`` aside, any existing parquet is treated as complete and the
dataset is skipped.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import queue
import time
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from parsimony_sdmx.cli.layout import agency_dir, series_parquet
from parsimony_sdmx.cli.memory_monitor import (
    MemoryMonitorConfig,
    clear_worker_marker,
    memory_monitor,
    read_oom_marker,
    write_worker_marker,
)
from parsimony_sdmx.cli.orphan_sweep import sweep_orphans
from parsimony_sdmx.core.outcomes import (
    DatasetOutcome,
    FailureKind,
    OutcomeStatus,
)
from parsimony_sdmx.io.http import classify_exception

logger = logging.getLogger(__name__)

_QUEUE_FLUSH_TIMEOUT_S = 2.0
"""Wait budget for the child's feeder thread to flush the outcome after clean exit.

The child calls ``close()`` + ``join_thread()`` before returning, so in
practice the outcome is already in the pipe. This timeout only protects
against scheduler-induced reorderings on very loaded systems.
"""

DEFAULT_PER_DATASET_TIMEOUT_S = 900.0
"""Wall-clock ceiling per dataset. Datasets exceeding this are killed
and surfaced as ``FailureKind.TIMEOUT`` — the run continues with the
remaining datasets rather than freezing on one pathological upstream.

The IMF ``series_keys`` endpoint can be very slow (6+ minutes for
large flows); ``sdmx1``'s XML parse adds another 30–60s on top. 15
minutes comfortably covers the legitimate slow cases while still
catching a truly hung child before it holds the run forever.
"""

WorkerFn = Callable[[str, str, str], DatasetOutcome]
"""Module-level worker ``(agency_id, dataset_id, output_base_str) -> outcome``.

Must be picklable — defined at module level, no closures, no lambdas.
"""


@dataclass(frozen=True, slots=True)
class OrchestratorConfig:
    memory: MemoryMonitorConfig | None = None
    force: bool = False
    per_dataset_timeout_s: float | None = DEFAULT_PER_DATASET_TIMEOUT_S
    isolate_subprocess: bool = True


def run_agency(
    agency_id: str,
    output_base: Path,
    dataset_ids: Sequence[str],
    worker_fn: WorkerFn,
    config: OrchestratorConfig | None = None,
) -> list[DatasetOutcome]:
    """Run ``worker_fn`` once per dataset, returning every outcome."""
    cfg = config or OrchestratorConfig()
    output_base = Path(output_base)
    agency_dir(output_base, agency_id).mkdir(parents=True, exist_ok=True)
    sweep_orphans(output_base, agency_id)

    to_process = _resume_filter(output_base, agency_id, dataset_ids, cfg.force)
    total = len(to_process)
    outcomes: list[DatasetOutcome] = []

    with memory_monitor(output_base, agency_id, cfg.memory):
        for idx, dataset_id in enumerate(to_process, start=1):
            logger.info(
                "[%d/%d] %s.%s processing", idx, total, agency_id, dataset_id
            )
            outcome = _run_single(
                agency_id,
                dataset_id,
                output_base,
                worker_fn,
                cfg,
            )
            logger.info(
                "[%d/%d] %s.%s %s (%.1fs)",
                idx,
                total,
                agency_id,
                dataset_id,
                outcome.status.value,
                outcome.duration_s,
            )
            outcomes.append(outcome)
    return outcomes


def _resume_filter(
    output_base: Path,
    agency_id: str,
    dataset_ids: Sequence[str],
    force: bool,
) -> list[str]:
    """Skip datasets whose canonical series parquet already exists.

    Atomic writes guarantee the parquet exists iff the prior run wrote
    it to completion, so filesystem alone is the resume contract.
    """
    if force:
        return list(dataset_ids)
    keep: list[str] = []
    skipped = 0
    for ds in dataset_ids:
        if series_parquet(output_base, agency_id, ds).exists():
            skipped += 1
        else:
            keep.append(ds)
    if skipped:
        logger.info("Resume: %d dataset(s) already written, skipping", skipped)
    return keep


def _run_single(
    agency_id: str,
    dataset_id: str,
    output_base: Path,
    worker_fn: WorkerFn,
    cfg: OrchestratorConfig,
) -> DatasetOutcome:
    started = datetime.now(UTC).isoformat()
    start_t = time.monotonic()
    if not cfg.isolate_subprocess:
        outcome = _invoke_inline(agency_id, dataset_id, output_base, worker_fn)
    else:
        outcome = _invoke_subprocess(
            agency_id,
            dataset_id,
            output_base,
            worker_fn,
            cfg.per_dataset_timeout_s,
        )
    finished = datetime.now(UTC).isoformat()
    duration = time.monotonic() - start_t
    return _with_timing(outcome, started, finished, duration)


def _invoke_inline(
    agency_id: str,
    dataset_id: str,
    output_base: Path,
    worker_fn: WorkerFn,
) -> DatasetOutcome:
    """Run the worker in-process — for tests and small runs only."""
    try:
        return worker_fn(agency_id, dataset_id, str(output_base))
    except Exception as exc:
        # Narrow to Exception so KeyboardInterrupt/SystemExit propagate
        # and Ctrl-C on an inline run actually interrupts.
        return _failure_from_exception(agency_id, dataset_id, exc)


def _invoke_subprocess(
    agency_id: str,
    dataset_id: str,
    output_base: Path,
    worker_fn: WorkerFn,
    timeout_s: float | None,
) -> DatasetOutcome:
    """Run the worker in a fresh subprocess, reap, synthesise outcome."""
    ctx = mp.get_context("spawn")
    result_q: mp.Queue[DatasetOutcome] = ctx.Queue()
    proc = ctx.Process(
        target=_child_entry,
        args=(result_q, worker_fn, agency_id, dataset_id, str(output_base)),
        daemon=False,
    )
    proc.start()
    proc.join(timeout=timeout_s)

    if proc.is_alive():
        logger.warning("Dataset %s exceeded timeout; terminating", dataset_id)
        proc.terminate()
        proc.join(timeout=5.0)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5.0)
        _drain_queue(result_q)
        return _failure(
            agency_id,
            dataset_id,
            FailureKind.TIMEOUT,
            f"Exceeded per-dataset timeout of {timeout_s}s",
        )

    exit_code = proc.exitcode
    child_pid = proc.pid

    if exit_code == 0:
        # mp.Queue is an async pipe — the child's feeder thread may still
        # be flushing pickled bytes after a clean exit. Block briefly so
        # a real outcome isn't mis-classified as "no outcome posted".
        try:
            outcome: DatasetOutcome = result_q.get(timeout=_QUEUE_FLUSH_TIMEOUT_S)
            _drain_queue(result_q)
            return outcome
        except queue.Empty:
            return _failure(
                agency_id,
                dataset_id,
                FailureKind.UNKNOWN,
                "Worker exited cleanly but posted no outcome",
            )

    # Non-zero exit — check for OOM marker.
    oom = read_oom_marker(output_base, agency_id, child_pid) if child_pid else None
    if oom is not None:
        rss = oom.get("rss_bytes")
        sys_pct = oom.get("system_memory_percent")
        phase = oom.get("phase")
        marker_ds = oom.get("dataset_id") or dataset_id
        msg = (
            f"OOM-killed (rss={rss} bytes, sys={sys_pct}%, phase={phase})"
        )
        return _failure(agency_id, marker_ds, FailureKind.OOM_KILLED, msg)

    return _failure(
        agency_id,
        dataset_id,
        FailureKind.UNKNOWN,
        f"Worker subprocess exited with code {exit_code}",
    )


def _child_entry(
    result_q: mp.Queue[DatasetOutcome],
    worker_fn: WorkerFn,
    agency_id: str,
    dataset_id: str,
    output_base_str: str,
) -> None:
    """Runs inside the forked/spawned child. Must be module-level and picklable."""
    output_base = Path(output_base_str)
    pid = os.getpid()
    write_worker_marker(output_base, agency_id, pid, dataset_id)
    try:
        outcome = worker_fn(agency_id, dataset_id, output_base_str)
    except Exception as exc:
        # KeyboardInterrupt/SystemExit propagate out so the child exits
        # non-zero on deliberate signals.
        outcome = _failure_from_exception(agency_id, dataset_id, exc)
    finally:
        clear_worker_marker(output_base, agency_id, pid)
    try:
        result_q.put(outcome)
        # Flush the feeder thread before returning so the parent's
        # bounded-block Queue.get doesn't race the pipe flush.
        result_q.close()
        result_q.join_thread()
    except Exception:
        logger.exception("Failed to post outcome for %s", dataset_id)


def _drain_queue(q: mp.Queue[DatasetOutcome]) -> None:
    """Drain any stragglers (e.g., leftover from a timed-out child that
    posted late) and release the queue's resources."""
    try:
        while True:
            q.get_nowait()
    except queue.Empty:
        pass
    try:
        q.close()
        q.join_thread()
    except Exception:
        logger.debug("Queue cleanup failed (benign)", exc_info=True)


def _failure(
    agency_id: str,
    dataset_id: str,
    kind: FailureKind,
    message: str,
) -> DatasetOutcome:
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.FAILED,
        kind=kind,
        error_message=message,
    )


def _failure_from_exception(
    agency_id: str,
    dataset_id: str,
    exc: BaseException,
) -> DatasetOutcome:
    return DatasetOutcome(
        dataset_id=dataset_id,
        agency_id=agency_id,
        status=OutcomeStatus.FAILED,
        kind=classify_exception(exc),
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )


def _with_timing(
    outcome: DatasetOutcome,
    started_at: str,
    finished_at: str,
    duration_s: float,
) -> DatasetOutcome:
    from dataclasses import replace

    return replace(
        outcome,
        started_at=started_at,
        finished_at=finished_at,
        duration_s=duration_s,
    )
