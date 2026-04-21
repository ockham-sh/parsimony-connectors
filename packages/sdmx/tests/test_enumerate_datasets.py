"""Tests for ``enumerate_sdmx_datasets`` — single-namespace catalog enumerator."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.catalog import entries_from_result

from parsimony_sdmx.connectors.enumerate_datasets import (
    DATASETS_NAMESPACE,
    EnumerateDatasetsParams,
    enumerate_sdmx_datasets,
)


def _write_datasets_parquet(root: Path, agency: str, rows: list[tuple[str, str, str]]) -> None:
    agency_dir = root / agency
    agency_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"dataset_id": did, "agency_id": aid, "title": title} for did, aid, title in rows],
        schema=pa.schema(
            [
                pa.field("dataset_id", pa.string(), nullable=False),
                pa.field("agency_id", pa.string(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
            ]
        ),
    )
    pq.write_table(table, agency_dir / "datasets.parquet")


@pytest.fixture
def outputs_root(tmp_path: Path) -> Path:
    _write_datasets_parquet(
        tmp_path, "ECB", [("YC", "ECB", "Euro Yield Curve"), ("MIR", "ECB", "Money Market Rates")]
    )
    _write_datasets_parquet(tmp_path, "ESTAT", [("prc_hicp_manr", "ESTAT", "HICP annual rate of change")])
    return tmp_path


@pytest.mark.asyncio
async def test_enumerates_all_agencies(outputs_root: Path) -> None:
    result = await enumerate_sdmx_datasets.bind_deps(outputs_root=outputs_root)(EnumerateDatasetsParams())
    df = result.data
    assert set(df["code"]) == {"ECB|YC", "ECB|MIR", "ESTAT|prc_hicp_manr"}
    assert set(df.columns) == {"code", "title", "agency", "dataset_id"}


@pytest.mark.asyncio
async def test_ingests_into_expected_namespace(outputs_root: Path) -> None:
    result = await enumerate_sdmx_datasets.bind_deps(outputs_root=outputs_root)(EnumerateDatasetsParams())
    output_config = enumerate_sdmx_datasets.output_config
    assert output_config is not None  # enumerator was declared with output=
    table = result.to_table(output_config)
    entries = entries_from_result(table)

    assert len(entries) == 3
    # Static namespace — every entry lands in sdmx_datasets.
    assert all(e.namespace == DATASETS_NAMESPACE for e in entries)
    assert {e.code for e in entries} == {"ECB|YC", "ECB|MIR", "ESTAT|prc_hicp_manr"}
    # Titles come through.
    codes_to_titles = {e.code: e.title for e in entries}
    assert codes_to_titles["ECB|YC"] == "Euro Yield Curve"


@pytest.mark.asyncio
async def test_missing_agencies_skipped_silently(tmp_path: Path) -> None:
    # Only ECB has a parquet; the rest should be skipped without error.
    _write_datasets_parquet(tmp_path, "ECB", [("X", "ECB", "X title")])
    result = await enumerate_sdmx_datasets.bind_deps(outputs_root=tmp_path)(EnumerateDatasetsParams())
    df = result.data
    assert list(df["code"]) == ["ECB|X"]


@pytest.mark.asyncio
async def test_empty_root_raises_emptydata(tmp_path: Path) -> None:
    from parsimony.errors import EmptyDataError

    with pytest.raises(EmptyDataError):
        await enumerate_sdmx_datasets.bind_deps(outputs_root=tmp_path)(EnumerateDatasetsParams())


def test_enumerator_metadata_shape() -> None:
    """The decorator registered a valid enumerator output."""
    output_config = enumerate_sdmx_datasets.output_config
    assert output_config is not None
    cols = output_config.columns
    key_cols = [c for c in cols if c.role.value == "key"]
    assert len(key_cols) == 1
    assert key_cols[0].namespace == "sdmx_datasets"
