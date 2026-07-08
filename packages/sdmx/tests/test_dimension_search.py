"""Tests for ``sdmx_dimension_search`` — per-dimension value search/enumeration.

Builds the tiny ECB/TEST flow (dimensions FREQ={M,A}, REF_AREA={DE,FR}) and drives the
connector against it: a ``query`` ranks values via the per-dimension index; omitting
``query`` enumerates every populated value; unpublished flows hard-error.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.catalog import BM25Index
from parsimony.errors import ConnectorError, InvalidParameterError

from parsimony_sdmx.catalog_series import build_flow_catalog
from parsimony_sdmx.connectors.dimension_search import sdmx_dimension_search
from parsimony_sdmx.connectors.series_search import _clear_series_catalog_lru
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.core.models import (
    CodelistCode,
    CodelistRecord,
    DimensionStructure,
    StructureRecord,
)
from parsimony_sdmx.series_fields import SERIES_PARQUET


def _structure() -> StructureRecord:
    return StructureRecord(
        dataset_id="TEST",
        agency_id="ECB",
        title="Test flow",
        dsd_order=("FREQ", "REF_AREA"),
        dimensions=(
            DimensionStructure(dimension_id="FREQ", codelist_id="CL_FREQ", name="Frequency", code_count=2),
            DimensionStructure(dimension_id="REF_AREA", codelist_id="CL_GEO", name="Reference area", code_count=2),
        ),
        codelists=(
            CodelistRecord(
                codelist_id="CL_FREQ",
                codes=(CodelistCode(code="M", label="Monthly"), CodelistCode(code="A", label="Annual")),
            ),
            CodelistRecord(
                codelist_id="CL_GEO",
                codes=(CodelistCode(code="DE", label="Germany"), CodelistCode(code="FR", label="France")),
            ),
        ),
    )


def _sample_table() -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "key": "M.DE",
                "title": "Monthly, Germany",
                "FREQ_code": "M",
                "FREQ_label": "Monthly",
                "REF_AREA_code": "DE",
                "REF_AREA_label": "Germany",
            },
            {
                "key": "A.DE",
                "title": "Annual, Germany",
                "FREQ_code": "A",
                "FREQ_label": "Annual",
                "REF_AREA_code": "DE",
                "REF_AREA_label": "Germany",
            },
            {
                "key": "M.FR",
                "title": "Monthly, France",
                "FREQ_code": "M",
                "FREQ_label": "Monthly",
                "REF_AREA_code": "FR",
                "REF_AREA_label": "France",
            },
        ]
    )


@pytest.fixture
def catalog_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(_sample_table(), parquet)
    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())
    catalogs_dir = tmp_path / "catalogs"
    build_flow_catalog(
        series_parquet=parquet,
        namespace="sdmx_series_ecb_test",
        agency=AgencyId.ECB,
        flow_id="TEST",
        structure=_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    _clear_series_catalog_lru()
    return str(catalogs_dir)


def test_enumerate_dimension_returns_all_values(catalog_root: str) -> None:
    df = sdmx_dimension_search(
        agency="ECB", dataset_id="TEST", dimension="FREQ", limit=10_000, catalog_root=catalog_root
    ).data
    assert dict(zip(df["code"], df["label"], strict=True)) == {"M": "Monthly", "A": "Annual"}


def test_enumerate_ref_area(catalog_root: str) -> None:
    df = sdmx_dimension_search(
        agency="ECB", dataset_id="TEST", dimension="REF_AREA", limit=10_000, catalog_root=catalog_root
    ).data
    assert set(df["code"]) == {"DE", "FR"}


def test_query_ranks_values_by_label(catalog_root: str) -> None:
    df = sdmx_dimension_search(
        agency="ECB", dataset_id="TEST", dimension="REF_AREA", query="Germany", limit=5, catalog_root=catalog_root
    ).data
    assert "DE" in set(df["code"])
    assert df.iloc[0]["code"] == "DE"


def test_ranked_query_rejects_enumeration_limit(catalog_root: str) -> None:
    with pytest.raises(InvalidParameterError, match="ranked shortlist"):
        sdmx_dimension_search(
            agency="ECB", dataset_id="TEST", dimension="FREQ", query="monthly", limit=5000, catalog_root=catalog_root
        )


def test_unknown_dimension_raises(catalog_root: str) -> None:
    with pytest.raises(InvalidParameterError, match="unknown dimension"):
        sdmx_dimension_search(agency="ECB", dataset_id="TEST", dimension="NOPE", catalog_root=catalog_root)


def test_unpublished_flow_hard_errors(catalog_root: str) -> None:
    with pytest.raises(ConnectorError, match="not published"):
        sdmx_dimension_search(agency="ECB", dataset_id="MISSING", dimension="FREQ", catalog_root=catalog_root)
