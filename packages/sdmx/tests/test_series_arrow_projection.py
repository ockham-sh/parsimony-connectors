"""Parity tests for the lean Arrow → Entity catalog build path."""

from __future__ import annotations

import pyarrow as pa
from parsimony.catalog.source import entities_from_raw
from parsimony.entity import Entity

from parsimony_sdmx.catalog_build import entities_from_series_arrow_table, manifest_from_series_entries
from parsimony_sdmx.catalog_policy import discover_dim_codes, sdmx_series_entries
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import _series_frame, _series_output_config
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord


def _sample_records() -> list[SeriesRecord]:
    return [
        SeriesRecord(
            id="M.DE.IF_1Y",
            dataset_id="YC",
            title="ECB source title",
            dimensions=(
                DimensionValue(id="FREQ", code="M", label="Monthly"),
                DimensionValue(id="REF_AREA", code="DE", label="Germany"),
            ),
        ),
        SeriesRecord(
            id="A.U2.SR_10Y",
            dataset_id="YC",
            title="Spot rate",
            dimensions=(
                DimensionValue(id="FREQ", code="A", label="Annual"),
                DimensionValue(id="REF_AREA", code="U2", label="Euro area"),
            ),
        ),
    ]


def _records_to_arrow(records: list[SeriesRecord]) -> pa.Table:
    rows = [
        {
            "id": record.id,
            "dataset_id": record.dataset_id,
            "title": record.title,
            "dimensions": [{"id": dim.id, "code": dim.code, "label": dim.label} for dim in record.dimensions],
        }
        for record in records
    ]
    return pa.Table.from_pylist(rows)


def _legacy_projection(records: list[SeriesRecord]) -> tuple[list[Entity], list[dict[str, object]]]:
    agency = AgencyId.ECB
    dataset_id = "YC"
    frame = _series_frame(records, agency=agency.value, dataset_id=dataset_id)
    raw_entries = entities_from_raw(frame, _series_output_config(agency, dataset_id))
    dim_codes = discover_dim_codes(raw_entries)
    manifest = manifest_from_series_entries(raw_entries)
    entries = sdmx_series_entries(raw_entries, dim_codes)
    return entries, manifest


def test_entities_from_series_arrow_table_matches_legacy_path() -> None:
    records = _sample_records()
    table = _records_to_arrow(records)

    lean_entries, lean_manifest = entities_from_series_arrow_table(
        table,
        agency=AgencyId.ECB,
        dataset_id="YC",
    )
    legacy_entries, legacy_manifest = _legacy_projection(records)

    assert len(lean_entries) == len(legacy_entries)
    for lean, legacy in zip(lean_entries, legacy_entries, strict=True):
        assert lean.namespace == legacy.namespace
        assert lean.code == legacy.code
        assert lean.title == legacy.title
        assert lean.metadata == legacy.metadata

    assert lean_manifest == legacy_manifest
