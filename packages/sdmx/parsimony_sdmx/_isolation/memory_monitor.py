"""Background thread that kills the largest child when system memory is high.

Preserved from the legacy module because per-dataset ``sdmx1 +
pandas`` spikes are real and the Pool's ``maxtasksperchild=1`` alone
doesn't save the box from OOM on concurrent heavy datasets.

Before issuing ``SIGKILL`` the monitor writes an OOM marker JSON into
``{agency_dir}/.oom/{pid}.json`` so the parent driver can classify the
victim as ``FailureKind.OOM_KILLED`` with the real peak RSS instead of
an opaque ``exit 137``.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

from parsimony_sdmx._isolation.layout import oom_dir

logger = logging.getLogger(__name__)

DEFAULT_POLL_SECONDS = 2.0
DEFAULT_THRESHOLD_PERCENT = 90.0
WORKER_MARKER_PREFIX = "worker."


@dataclass(frozen=True, slots=True)
class MemoryMonitorConfig:
    threshold_percent: float = DEFAULT_THRESHOLD_PERCENT
    poll_seconds: float = DEFAULT_POLL_SECONDS


def write_worker_marker(
    output_base: Path,
    agency_id: str,
    pid: int,
    dataset_id: str,
    phase: str = "running",
) -> Path:
    """Worker registers itself so the monitor knows which dataset it's on."""
    d = oom_dir(output_base, agency_id)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{WORKER_MARKER_PREFIX}{pid}.json"
    payload = {
        "dataset_id": dataset_id,
        "pid": pid,
        "phase": phase,
        "started_at": datetime.now(UTC).isoformat(),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def clear_worker_marker(output_base: Path, agency_id: str, pid: int) -> None:
    """Remove the self-registration file after a clean exit."""
    path = oom_dir(output_base, agency_id) / f"{WORKER_MARKER_PREFIX}{pid}.json"
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        logger.warning("Failed to clear worker marker %s", path, exc_info=True)


def read_oom_marker(
    output_base: Path,
    agency_id: str,
    pid: int,
) -> dict[str, Any] | None:
    """Parent reads the monitor-written marker after reaping a SIGKILL'd child."""
    path = oom_dir(output_base, agency_id) / f"{pid}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        logger.warning("Unreadable OOM marker %s", path, exc_info=True)
    return None


def _read_worker_marker(
    output_base: Path,
    agency_id: str,
    pid: int,
) -> dict[str, Any] | None:
    path = oom_dir(output_base, agency_id) / f"{WORKER_MARKER_PREFIX}{pid}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _write_oom_marker(
    output_base: Path,
    agency_id: str,
    pid: int,
    rss_bytes: int,
    system_percent: float,
    worker_data: dict[str, Any] | None,
) -> None:
    d = oom_dir(output_base, agency_id)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{pid}.json"
    payload = {
        "pid": pid,
        "rss_bytes": rss_bytes,
        "system_memory_percent": system_percent,
        "decided_at": datetime.now(UTC).isoformat(),
        "caused_by": "memory_monitor",
        "dataset_id": (worker_data or {}).get("dataset_id"),
        "phase": (worker_data or {}).get("phase"),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _kill_largest_child(
    parent: psutil.Process,
    output_base: Path,
    agency_id: str,
    system_percent: float,
) -> bool:
    """Return True if a child was killed."""
    try:
        children = parent.children(recursive=True)
    except psutil.Error:
        return False

    largest: psutil.Process | None = None
    largest_rss = -1
    for child in children:
        try:
            rss = child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if rss > largest_rss:
            largest_rss = rss
            largest = child

    if largest is None or largest_rss < 0:
        return False

    worker = _read_worker_marker(output_base, agency_id, largest.pid)
    _write_oom_marker(
        output_base,
        agency_id,
        largest.pid,
        largest_rss,
        system_percent,
        worker,
    )
    logger.warning(
        "Killing PID %s (rss=%.1f MB, sys=%.1f%%, dataset=%s)",
        largest.pid,
        largest_rss / 1024 / 1024,
        system_percent,
        (worker or {}).get("dataset_id"),
    )
    try:
        largest.send_signal(signal.SIGKILL)
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def _monitor_loop(
    stop: threading.Event,
    output_base: Path,
    agency_id: str,
    config: MemoryMonitorConfig,
) -> None:
    parent = psutil.Process()
    while not stop.wait(config.poll_seconds):
        try:
            mem = psutil.virtual_memory()
        except psutil.Error:
            continue
        if mem.percent < config.threshold_percent:
            continue
        _kill_largest_child(parent, output_base, agency_id, mem.percent)


@contextmanager
def memory_monitor(
    output_base: Path,
    agency_id: str,
    config: MemoryMonitorConfig | None = None,
) -> Iterator[None]:
    """Run the monitor thread for the duration of the ``with`` block."""
    cfg = config or MemoryMonitorConfig()
    stop = threading.Event()
    thread = threading.Thread(
        target=_monitor_loop,
        args=(stop, output_base, agency_id, cfg),
        name="parsimony-sdmx-memory-monitor",
        daemon=True,
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=cfg.poll_seconds + 1.0)


def slow_sleep(seconds: float) -> None:  # pragma: no cover - legacy parity helper
    """Sleep — used by tests that need a pacing beat without pulling time."""
    time.sleep(seconds)
