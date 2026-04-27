"""Subprocess isolation boundary for every ``sdmx1``-touching call.

**Why this package exists.** ``sdmx1`` caches parsed structure messages
(DSDs, codelists, dataflows) at module scope with no public invalidation
API. A long-lived Python process that imports it accumulates cache
monotonically — tens to hundreds of MB per dataset fetch, never
released. Over a full publish run (dozens of datasets across four
agencies) the parent OOMs.

Process death is the only way to flush that cache. Every sdmx1-touching
call — ``list_datasets`` (agency dataflow listing) and ``fetch_series``
(per-dataset series sweep) — runs in a freshly spawned subprocess that
is discarded after the call. **Never pooled** — a ``ProcessPoolExecutor``
would retain sdmx1 in each worker across tasks and defeat the invariant.

The two entry points handle their payload sizes differently:

* :func:`list_datasets` (``listing.py``): small payload (up to ~8 k
  dataflow tuples), returned via ``mp.Queue``. The queue MUST be drained
  before ``proc.join()`` — the feeder thread blocks on the OS pipe
  buffer once the pickled bytes exceed ~64 KB, and ``join_thread()``
  never returns until the parent reads.
* :func:`fetch_series` (``worker.py`` + this module): potentially huge
  payload (ECB YC has ~2 k series; ESTAT UNE_RT_M has ~2 k). The child
  writes the parquet to a caller-supplied tmpdir and returns only a
  :class:`~parsimony_sdmx.core.outcomes.DatasetOutcome` envelope (a few
  hundred bytes). The parent reads the parquet back after the child
  exits. No queue-size worry; disk is the transport.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from parsimony_sdmx._isolation.layout import series_parquet
from parsimony_sdmx._isolation.listing import (
    DEFAULT_TIMEOUT_S as LIST_DEFAULT_TIMEOUT_S,
)
from parsimony_sdmx._isolation.listing import (
    ListDatasetsError,
    list_datasets,
    run_in_child,
)
from parsimony_sdmx._isolation.worker import run_dataset
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.core.outcomes import DatasetOutcome, FailureKind, OutcomeStatus

FETCH_SERIES_DEFAULT_TIMEOUT_S = 900.0
"""Upper bound for a per-dataset series sweep.

A large SDMX fetch (DSD resolution + codelist download + series
enumeration) typically completes in 30–120 s. 15 min gives slack for
transient upstream slowness without letting a hung child hold a
publish run indefinitely.
"""


class FetchSeriesError(RuntimeError):
    """Raised when the per-dataset subprocess failed (or returned no parquet).

    Wraps a :class:`DatasetOutcome` with ``status == FAILED`` so callers
    can log its ``kind`` / ``error_message`` / ``traceback`` just like
    the old batch orchestrator did.
    """

    def __init__(self, outcome: DatasetOutcome) -> None:
        super().__init__(outcome.error_message or f"{outcome.status.value}")
        self.outcome = outcome


def fetch_series(
    agency_id: str,
    dataset_id: str,
    timeout_s: float = FETCH_SERIES_DEFAULT_TIMEOUT_S,
) -> list[SeriesRecord]:
    """Run ``provider.list_series(dataset_id)`` in an isolated subprocess.

    The child writes the series to a parquet file inside a fresh
    :class:`tempfile.TemporaryDirectory`; the parent reads them back
    after the child exits. The tmpdir is deleted on return, so there's
    no on-disk state leaking into the caller's environment.

    Raises
    ------
    FetchSeriesError
        If the subprocess raised, timed out, or produced no parquet.
        The underlying :class:`DatasetOutcome` is attached as ``.outcome``.
    """
    with tempfile.TemporaryDirectory(prefix="parsimony-sdmx-") as td:
        output_base = Path(td)
        try:
            outcome = run_in_child(
                _fetch_series_child,
                (agency_id, dataset_id, str(output_base)),
                timeout_s,
                f"{agency_id}/{dataset_id}",
            )
        except ListDatasetsError as exc:
            # ``run_in_child`` is shared with the listing path and raises its
            # own error type; rewrap as FetchSeriesError so callers only need
            # to handle one exception class per entry point.
            kind = FailureKind.TIMEOUT if exc.kind == "timeout" else FailureKind.UNKNOWN
            raise FetchSeriesError(
                DatasetOutcome(
                    dataset_id=dataset_id,
                    agency_id=agency_id,
                    status=OutcomeStatus.FAILED,
                    kind=kind,
                    error_message=exc.message,
                    traceback=exc.traceback_str,
                )
            ) from exc

        if not isinstance(outcome, DatasetOutcome):
            raise FetchSeriesError(
                DatasetOutcome(
                    dataset_id=dataset_id,
                    agency_id=agency_id,
                    status=OutcomeStatus.FAILED,
                    error_message=(
                        f"subprocess returned unexpected payload: {type(outcome).__name__}"
                    ),
                )
            )
        if outcome.status == OutcomeStatus.FAILED:
            raise FetchSeriesError(outcome)

        parquet_path = series_parquet(output_base, agency_id, dataset_id)
        if not parquet_path.exists():
            raise FetchSeriesError(
                DatasetOutcome(
                    dataset_id=dataset_id,
                    agency_id=agency_id,
                    status=OutcomeStatus.FAILED,
                    error_message=(
                        f"subprocess reported {outcome.status.value} but produced no parquet at {parquet_path}"
                    ),
                )
            )

        table = pq.read_table(parquet_path)
        records: list[SeriesRecord] = []
        for row in table.to_pylist():
            raw_fragments = row.get("fragments") or ()
            records.append(
                SeriesRecord(
                    id=row["id"],
                    dataset_id=row["dataset_id"],
                    title=row["title"],
                    fragments=tuple(str(f) for f in raw_fragments),
                )
            )
        return records


def _fetch_series_child(
    result_q,
    agency_id: str,
    dataset_id: str,
    output_base_str: str,
) -> None:
    """Child-process entry point: run the worker, put the outcome on the queue.

    Module-level so ``multiprocessing.spawn`` can pickle it. Every
    sdmx1-touching import happens lazily inside ``run_dataset`` — the
    parent address space never loads sdmx1.
    """
    import contextlib

    try:
        outcome = run_dataset(agency_id, dataset_id, output_base_str)
        result_q.put(outcome)
    except Exception as exc:
        import traceback as _tb

        from parsimony_sdmx.io.http import classify_exception

        result_q.put(
            DatasetOutcome(
                dataset_id=dataset_id,
                agency_id=agency_id,
                status=OutcomeStatus.FAILED,
                kind=classify_exception(exc),
                error_message=f"{type(exc).__name__}: {exc}",
                traceback=_tb.format_exc(),
            )
        )
    finally:
        with contextlib.suppress(Exception):
            result_q.close()
            result_q.join_thread()


__all__ = [
    "FETCH_SERIES_DEFAULT_TIMEOUT_S",
    "FetchSeriesError",
    "LIST_DEFAULT_TIMEOUT_S",
    "ListDatasetsError",
    "fetch_series",
    "list_datasets",
]
