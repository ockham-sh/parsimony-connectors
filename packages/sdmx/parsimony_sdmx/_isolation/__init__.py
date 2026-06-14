"""Subprocess isolation boundary for every ``sdmx1``-touching call.

**Why this package exists.** ``sdmx1`` caches parsed structure messages
(DSDs, codelists, dataflows) at module scope with no public invalidation
API. A long-lived Python process that imports it accumulates cache
monotonically — tens to hundreds of MB per structure fetch, never
released. Over a full publish run (dozens of datasets across four
agencies) the parent OOMs.

Process death is the only way to flush that cache. Every sdmx1-touching
call — ``list_datasets`` (agency dataflow listing) and ``fetch_structure``
(per-dataset DSD + codelists) — runs in a freshly spawned subprocess that
is discarded after the call. **Never pooled** — a ``ProcessPoolExecutor``
would retain sdmx1 in each worker across tasks and defeat the invariant.

The two entry points handle their payload sizes differently:

* :func:`list_datasets` (``listing.py``): small payload (up to ~8 k
  dataflow tuples), returned via ``mp.Queue``. The queue MUST be drained
  before ``proc.join()`` — the feeder thread blocks on the OS pipe
  buffer once the pickled bytes exceed ~64 KB, and ``join_thread()``
  never returns until the parent reads.
* :func:`fetch_structure` (``structure_worker.py`` + this module): moderate
  payload (DSD dimensions + codelist samples). The child writes JSON to
  a caller-supplied tmpdir and returns only a
  :class:`~parsimony_sdmx.core.outcomes.DatasetOutcome` envelope. The
  parent reads the JSON back after the child exits.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from parsimony_sdmx._isolation.listing import (
    DEFAULT_TIMEOUT_S as LIST_DEFAULT_TIMEOUT_S,
)
from parsimony_sdmx._isolation.listing import (
    ListDatasetsError,
    list_datasets,
    run_in_child,
)
from parsimony_sdmx.core.models import StructureRecord
from parsimony_sdmx.core.outcomes import DatasetOutcome, FailureKind, OutcomeStatus

FETCH_STRUCTURE_DEFAULT_TIMEOUT_S = 120.0
"""Upper bound for a per-dataset structure fetch (DSD + codelists only)."""


class FetchStructureError(RuntimeError):
    """Raised when the structure subprocess failed."""

    def __init__(self, outcome: DatasetOutcome) -> None:
        super().__init__(outcome.error_message or f"{outcome.status.value}")
        self.outcome = outcome


def fetch_structure(
    agency_id: str,
    dataset_id: str,
    timeout_s: float = FETCH_STRUCTURE_DEFAULT_TIMEOUT_S,
) -> StructureRecord:
    """Run ``provider.fetch_structure(dataset_id)`` in an isolated subprocess."""
    from parsimony_sdmx._isolation.layout import structure_json
    from parsimony_sdmx.io.structure_json import read_structure

    with tempfile.TemporaryDirectory(prefix="parsimony-sdmx-structure-") as td:
        output_base = Path(td)
        try:
            outcome = run_in_child(
                _fetch_structure_child,
                (agency_id, dataset_id, str(output_base)),
                timeout_s,
                f"{agency_id}/{dataset_id}:structure",
            )
        except ListDatasetsError as exc:
            kind = FailureKind.TIMEOUT if exc.kind == "timeout" else FailureKind.UNKNOWN
            raise FetchStructureError(
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
            raise FetchStructureError(
                DatasetOutcome(
                    dataset_id=dataset_id,
                    agency_id=agency_id,
                    status=OutcomeStatus.FAILED,
                    error_message=(f"subprocess returned unexpected payload: {type(outcome).__name__}"),
                )
            )
        if outcome.status == OutcomeStatus.FAILED:
            raise FetchStructureError(outcome)

        json_path = structure_json(output_base, agency_id, dataset_id)
        if not json_path.exists():
            raise FetchStructureError(
                DatasetOutcome(
                    dataset_id=dataset_id,
                    agency_id=agency_id,
                    status=OutcomeStatus.FAILED,
                    error_message=f"subprocess reported OK but produced no structure at {json_path}",
                )
            )
        return read_structure(json_path)


def _fetch_structure_child(
    result_q,
    agency_id: str,
    dataset_id: str,
    output_base_str: str,
) -> None:
    import contextlib

    from parsimony_sdmx._isolation.structure_worker import run_structure

    try:
        outcome = run_structure(agency_id, dataset_id, output_base_str)
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
    "FETCH_STRUCTURE_DEFAULT_TIMEOUT_S",
    "FetchStructureError",
    "LIST_DEFAULT_TIMEOUT_S",
    "ListDatasetsError",
    "fetch_structure",
    "list_datasets",
]
