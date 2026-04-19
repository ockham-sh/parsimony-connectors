"""Run ``provider.list_datasets()`` in a fresh subprocess.

The parent orchestrator stays sdmx1-free: every provider-library
invocation happens in a spawned child whose interpreter dies after the
work is done, taking the accumulated ``sdmx1`` module-level cache with
it. The parent receives only picklable ``DatasetRecord`` tuples.

Motivation: ``sdmx1`` caches structure messages at module scope with
no public invalidation hook. A long-lived parent that imports it pays
that cache-retention cost for the whole run. Spawning for the listing
call mirrors what :mod:`parsimony_sdmx.cli.orchestrator` already does
per dataset — same hygiene, applied upfront.
"""

from __future__ import annotations

import contextlib
import multiprocessing as mp
import queue
from collections.abc import Callable
from typing import Any

from parsimony_sdmx.core.models import DatasetRecord

DEFAULT_TIMEOUT_S = 600.0
"""Upper bound on the child's wall-clock time.

A single upstream dataflow listing rarely takes more than 1–2 minutes
even for ESTAT (8 k+ dataflows in one response). 10 minutes gives
plenty of slack for flaky upstreams without letting a hung child
hold the run forever.
"""


class ListDatasetsError(RuntimeError):
    """Raised when the listing subprocess failed or timed out.

    ``kind`` mirrors :class:`parsimony_sdmx.core.outcomes.FailureKind`'s
    string values (e.g. ``"http_error"``, ``"timeout"``) so callers can
    log a classification consistent with per-dataset outcomes.
    """

    def __init__(self, kind: str, message: str, traceback_str: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.traceback_str = traceback_str


def list_datasets(
    agency_id: str,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> list[DatasetRecord]:
    """Fetch the dataset catalog for ``agency_id`` in an isolated subprocess.

    Raises :class:`ListDatasetsError` on timeout, child crash, or any
    exception raised inside the child (the child classifies via
    :func:`parsimony_sdmx.io.http.classify_exception` before sending
    the result back, so callers can log a ``FailureKind``-shaped tag).
    """
    payload = run_in_child(_child_entry, (agency_id,), timeout_s, agency_id)
    status = payload[0]
    if status == "ok":
        tuples: list[tuple[str, str, str]] = payload[1]
        return [
            DatasetRecord(dataset_id=d, agency_id=a, title=t) for d, a, t in tuples
        ]

    _, kind, message, tb = payload
    raise ListDatasetsError(kind=kind, message=message, traceback_str=tb)


def run_in_child(
    child_fn: Callable[..., None],
    child_args: tuple[Any, ...],
    timeout_s: float,
    label: str,
) -> Any:
    """Spawn ``child_fn(result_q, *child_args)`` and return its single payload.

    The parent MUST read the queue before joining the child. For large
    payloads (ESTAT emits ~8k dataflows) the pickled tuples exceed the
    OS pipe buffer (~64 KB); the child's feeder thread blocks on the
    write, ``join_thread()`` never returns, and the child can't exit —
    deadlocking any prior ``proc.join()``. Reading first lets the pipe
    drain so the feeder can finish.
    """
    ctx = mp.get_context("spawn")
    result_q: mp.Queue[Any] = ctx.Queue()
    proc = ctx.Process(
        target=child_fn,
        args=(result_q, *child_args),
        daemon=False,
    )
    proc.start()

    try:
        payload = result_q.get(timeout=timeout_s)
    except queue.Empty:
        _terminate(proc)
        _drain_and_close(result_q)
        raise ListDatasetsError(
            kind="timeout",
            message=f"{label} exceeded timeout of {timeout_s}s",
            traceback_str="",
        ) from None

    proc.join(timeout=5.0)
    if proc.is_alive():
        _terminate(proc)
    _drain_and_close(result_q)
    return payload


def _terminate(proc: mp.process.BaseProcess) -> None:
    """SIGTERM, then SIGKILL if the child refuses to die within 5s."""
    if not proc.is_alive():
        return
    proc.terminate()
    proc.join(timeout=5.0)
    if proc.is_alive():
        proc.kill()
        proc.join(timeout=5.0)


def _child_entry(
    result_q: mp.Queue[Any],
    agency_id: str,
) -> None:
    """Runs inside the spawned child. Must be module-level and picklable.

    Every sdmx1-touching import lives inside this function so the
    parent's address space never loads it.
    """
    import traceback

    try:
        # Deferred imports — child-only, never reached in the parent.
        from parsimony_sdmx.providers.registry import get_provider

        provider = get_provider(agency_id)
        records = list(provider.list_datasets())
        payload = [(r.dataset_id, r.agency_id, r.title) for r in records]
        result_q.put(("ok", payload))
    except Exception as exc:
        try:
            from parsimony_sdmx.io.http import classify_exception

            kind = classify_exception(exc).value
        except Exception:
            kind = "unknown"
        result_q.put(
            (
                "err",
                kind,
                f"{type(exc).__name__}: {exc}",
                traceback.format_exc(),
            )
        )
    finally:
        with contextlib.suppress(Exception):
            result_q.close()
            result_q.join_thread()


def _drain_and_close(q: mp.Queue[Any]) -> None:
    with contextlib.suppress(queue.Empty):
        while True:
            q.get_nowait()
    with contextlib.suppress(Exception):
        q.close()
        q.join_thread()
