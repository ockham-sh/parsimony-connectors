"""Tests for SDMX dataset catalog assembly helpers."""

from __future__ import annotations

import pytest
from parsimony.catalog import CatalogEntry

from parsimony_sdmx.catalog_build import (
    build_agency_dataset_entries,
    dataset_code,
    enrich_dataset_entries,
    manifest_from_series_entries,
    merge_dataset_entry_lists,
)
from parsimony_sdmx.core.models import DatasetRecord


def _series_entries() -> list[CatalogEntry]:
    return [
        CatalogEntry(
            namespace="sdmx_series_ecb_yc",
            code="M.DE.IF_1Y",
            title="Synthetic title",
            metadata={
                "agency": "ECB",
                "dataset_id": "YC",
                "FREQ_code": "M",
                "FREQ_label": "Monthly",
                "REF_AREA_code": "DE",
                "REF_AREA_label": "Germany",
            },
        )
    ]


@pytest.mark.asyncio
async def test_build_agency_dataset_entries_only_updates_manifested_flows() -> None:
    records = [
        DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield curve"),
        DatasetRecord(dataset_id="MIR", agency_id="ECB", title="Money market"),
    ]
    manifests = {
        dataset_code("ECB", "YC"): manifest_from_series_entries(_series_entries()),
    }

    entries = await build_agency_dataset_entries(records, manifests)

    assert len(entries) == 1
    assert entries[0].code == "ECB|YC"
    assert entries[0].metadata["dimensions"][0]["id"] == "FREQ"


def test_enrich_dataset_entries_preserves_entries_without_manifest() -> None:
    base = CatalogEntry(
        namespace="sdmx_datasets",
        code="ECB|MIR",
        title="Money market",
        metadata={"agency": "ECB", "dataset_id": "MIR"},
    )
    enriched = enrich_dataset_entries([base], {})

    assert enriched[0].metadata == base.metadata


def test_merge_dataset_entry_lists_upserts_by_code() -> None:
    existing = [
        CatalogEntry(
            namespace="sdmx_datasets",
            code="ECB|MIR",
            title="Old title",
            metadata={"agency": "ECB", "dataset_id": "MIR"},
        )
    ]
    updates = [
        CatalogEntry(
            namespace="sdmx_datasets",
            code="ECB|YC",
            title="Yield curve",
            metadata={"agency": "ECB", "dataset_id": "YC", "dimensions": [{"id": "FREQ", "values": []}]},
        )
    ]

    merged = merge_dataset_entry_lists(existing, updates)

    assert {(e.code, e.title) for e in merged} == {("ECB|MIR", "Old title"), ("ECB|YC", "Yield curve")}
