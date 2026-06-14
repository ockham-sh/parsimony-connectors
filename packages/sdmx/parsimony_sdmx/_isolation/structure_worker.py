"""Structure-only worker run inside an isolated subprocess."""

from __future__ import annotations

import logging
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

from parsimony_sdmx.core.outcomes import DatasetOutcome, OutcomeStatus
from parsimony_sdmx.io.http import classify_exception
from parsimony_sdmx.io.structure_json import write_structure

logger = logging.getLogger(__name__)


def run_structure(
    agency_id: str,
    dataset_id: str,
    output_base_str: str,
) -> DatasetOutcome:
    from parsimony_sdmx._isolation.layout import structure_json
    from parsimony_sdmx.providers.registry import get_provider

    output_base = Path(output_base_str)
    started = datetime.now(UTC).isoformat()
    start_t = time.monotonic()
    try:
        provider = get_provider(agency_id)
        record = provider.fetch_structure(dataset_id)
        json_path = structure_json(output_base, agency_id, dataset_id)
        write_structure(record, json_path)
        size_bytes = json_path.stat().st_size
        return DatasetOutcome(
            dataset_id=dataset_id,
            agency_id=agency_id,
            status=OutcomeStatus.OK,
            rows=len(record.codelists),
            bytes=size_bytes,
            parquet_path=str(json_path),
            duration_s=time.monotonic() - start_t,
            started_at=started,
            finished_at=datetime.now(UTC).isoformat(),
        )
    except Exception as exc:
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
