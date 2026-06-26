"""Parquet writers with explicit typed schemas, uniqueness checks, and atomic writes.

Layout contract for SDMX catalog builders:

    outputs/{AGENCY}/datasets.parquet

Writes land in ``outputs/{AGENCY}/.tmp/`` first, are flushed and fsynced,
then ``os.replace()``-d into the canonical path. A killed writer leaves
an orphan in ``.tmp/``, never a truncated canonical parquet.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.io.paths import ensure_within, safe_filename

logger = logging.getLogger(__name__)

DATASETS_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("agency_id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
    ]
)

TMP_SUBDIR = ".tmp"
COMPRESSION = "zstd"


def _atomic_write(
    agency_dir: Path,
    final_path: Path,
    writer_fn: Callable[[Path], None],
) -> None:
    """Run ``writer_fn`` against a temp path, fsync, then ``os.replace``.

    Temp files live at ``{agency_dir}/.tmp/`` so the orchestrator's
    orphan sweep can find and reap them per agency.
    """
    ensure_within(agency_dir, final_path.parent)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_dir = agency_dir / TMP_SUBDIR
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_name = f"{final_path.name}.{os.getpid()}.{uuid.uuid4().hex}"
    tmp_path = tmp_dir / tmp_name

    try:
        writer_fn(tmp_path)
        with open(tmp_path, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)
    except BaseException:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                logger.exception("Failed to clean up temp file %s", tmp_path)
        raise


def write_datasets(
    records: Iterable[DatasetRecord],
    output_base: Path,
    agency_id: str,
) -> Path:
    """Write ``outputs/{agency_id}/datasets.parquet`` atomically.

    Asserts ``(agency_id, dataset_id)`` is unique across ``records``
    before the write.
    """
    safe_agency = safe_filename(agency_id)
    agency_dir = output_base / safe_agency
    final_path = agency_dir / "datasets.parquet"

    rows = list(records)
    _assert_unique_datasets(rows)
    table = pa.table(
        {
            "dataset_id": [r.dataset_id for r in rows],
            "agency_id": [r.agency_id for r in rows],
            "title": [r.title for r in rows],
        },
        schema=DATASETS_SCHEMA,
    )

    def _do_write(tmp_path: Path) -> None:
        pq.write_table(table, tmp_path, compression=COMPRESSION)

    _atomic_write(agency_dir, final_path, _do_write)
    return final_path


def _assert_unique_datasets(rows: list[DatasetRecord]) -> None:
    seen: set[tuple[str, str]] = set()
    for r in rows:
        key = (r.agency_id, r.dataset_id)
        if key in seen:
            raise ValueError(f"Duplicate (agency_id, dataset_id) in datasets: {key}")
        seen.add(key)
