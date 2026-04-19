"""Per-dataset worker run inside an isolated subprocess by the orchestrator.

Must be at module level so ``multiprocessing`` can pickle it. Must catch
every exception and always return a :class:`DatasetOutcome` — never raise
across the Pool boundary.
"""

from __future__ import annotations

import logging
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from parsimony_sdmx.core.outcomes import (
    DatasetOutcome,
    OutcomeStatus,
)
from parsimony_sdmx.io.http import classify_exception
from parsimony_sdmx.io.parquet import write_series

# NOTE: ``get_provider`` is imported lazily inside :func:`run_dataset`
# below. Importing it at module scope would drag ``sdmx1`` into every
# process that references the worker function — including the parent
# orchestrator, which only needs a pickleable reference to dispatch.

logger = logging.getLogger(__name__)


def run_dataset(
    agency_id: str,
    dataset_id: str,
    output_base_str: str,
) -> DatasetOutcome:
    """Fetch all series for ``dataset_id`` and write the series parquet."""
    # Lazy import — keeps sdmx1 out of the parent's address space.
    from parsimony_sdmx.providers.registry import get_provider

    output_base = Path(output_base_str)
    started = datetime.now(UTC).isoformat()
    start_t = time.monotonic()
    try:
        provider = get_provider(agency_id)
        series_iter = provider.list_series(dataset_id)
        parquet_path = write_series(
            series_iter, output_base, agency_id, dataset_id
        )
        rows = pq.ParquetFile(parquet_path).metadata.num_rows
        size_bytes = parquet_path.stat().st_size
        status = OutcomeStatus.OK if rows > 0 else OutcomeStatus.EMPTY
        return DatasetOutcome(
            dataset_id=dataset_id,
            agency_id=agency_id,
            status=status,
            rows=rows,
            bytes=size_bytes,
            parquet_path=str(parquet_path),
            duration_s=time.monotonic() - start_t,
            started_at=started,
            finished_at=datetime.now(UTC).isoformat(),
        )
    except Exception as exc:
        # Narrow to Exception — KeyboardInterrupt and SystemExit propagate
        # so operators can interrupt a stuck run and deliberate exits
        # aren't buried under a "failed dataset" outcome.
        kind = classify_exception(exc)
        return DatasetOutcome(
            dataset_id=dataset_id,
            agency_id=agency_id,
            status=OutcomeStatus.FAILED,
            kind=kind,
            error_message=str(exc),
            traceback=traceback.format_exc(),
            duration_s=time.monotonic() - start_t,
            started_at=started,
            finished_at=datetime.now(UTC).isoformat(),
        )
