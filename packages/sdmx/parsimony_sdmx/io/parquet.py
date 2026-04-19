"""Parquet writers with explicit typed schemas, uniqueness checks, and atomic writes.

Layout contract (public API to the downstream FAISS indexer):

    outputs/{AGENCY}/datasets.parquet
    outputs/{AGENCY}/series/{DATASET}.parquet

Writes land in ``outputs/{AGENCY}/.tmp/`` first, are flushed and fsynced,
then ``os.replace()``-d into the canonical path. A killed writer leaves
an orphan in ``.tmp/``, never a truncated canonical parquet.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord
from parsimony_sdmx.io.paths import ensure_within, safe_filename

logger = logging.getLogger(__name__)

DATASETS_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("agency_id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
    ]
)

SERIES_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
    ]
)

DEFAULT_BATCH_SIZE = 50_000
TMP_SUBDIR = ".tmp"
COMPRESSION = "zstd"


def _atomic_write(
    agency_dir: Path,
    final_path: Path,
    writer_fn: Callable[[Path], None],
) -> None:
    """Run ``writer_fn`` against a temp path, fsync, then ``os.replace``.

    Temp files live at ``{agency_dir}/.tmp/`` so the orchestrator's
    orphan sweep in T11 can find and reap them per agency.
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


def write_series(
    records: Iterable[SeriesRecord],
    output_base: Path,
    agency_id: str,
    dataset_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Path:
    """Stream ``records`` into ``outputs/{agency}/series/{dataset}.parquet`` atomically.

    Uses a ``ParquetWriter`` with batched ``RecordBatch`` flushes so peak
    memory is bounded by ``batch_size`` regardless of total row count.
    Asserts each series ``id`` is unique within the file and that every
    record's ``dataset_id`` matches the expected value.
    """
    safe_agency = safe_filename(agency_id)
    safe_dataset = safe_filename(dataset_id)
    agency_dir = output_base / safe_agency
    final_path = agency_dir / "series" / f"{safe_dataset}.parquet"

    def _do_write(tmp_path: Path) -> None:
        _write_series_batched(
            records=records,
            out_path=tmp_path,
            dataset_id=dataset_id,
            batch_size=batch_size,
        )

    _atomic_write(agency_dir, final_path, _do_write)
    return final_path


def _write_series_batched(
    records: Iterable[SeriesRecord],
    out_path: Path,
    dataset_id: str,
    batch_size: int,
) -> None:
    writer = pq.ParquetWriter(out_path, SERIES_SCHEMA, compression=COMPRESSION)
    seen_ids: set[str] = set()
    try:
        for batch_rows in _chunked(records, batch_size):
            ids: list[str] = []
            dsids: list[str] = []
            titles: list[str] = []
            for rec in batch_rows:
                if rec.dataset_id != dataset_id:
                    raise ValueError(
                        f"SeriesRecord.dataset_id={rec.dataset_id!r} "
                        f"does not match expected {dataset_id!r}"
                    )
                if rec.id in seen_ids:
                    raise ValueError(
                        f"Duplicate series id in dataset {dataset_id!r}: {rec.id!r}"
                    )
                seen_ids.add(rec.id)
                ids.append(rec.id)
                dsids.append(rec.dataset_id)
                titles.append(rec.title)
            if ids:
                batch = pa.RecordBatch.from_pydict(
                    {"id": ids, "dataset_id": dsids, "title": titles},
                    schema=SERIES_SCHEMA,
                )
                writer.write_batch(batch)
    finally:
        writer.close()


def _chunked(records: Iterable[SeriesRecord], n: int) -> Iterator[list[SeriesRecord]]:
    if n <= 0:
        raise ValueError(f"batch_size must be > 0, got {n}")
    buf: list[SeriesRecord] = []
    for rec in records:
        buf.append(rec)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def _assert_unique_datasets(rows: list[DatasetRecord]) -> None:
    seen: set[tuple[str, str]] = set()
    for r in rows:
        key = (r.agency_id, r.dataset_id)
        if key in seen:
            raise ValueError(f"Duplicate (agency_id, dataset_id) in datasets: {key}")
        seen.add(key)
