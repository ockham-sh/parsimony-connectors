"""Tests for SDMX dataset catalog assembly helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from parsimony.entity import Entity

from parsimony_sdmx.catalog_build import (
    build_agency_dataset_entities,
    dataset_code,
    enrich_dataset_entities,
    manifest_from_saved_series,
    manifest_from_series_entries,
    merge_dataset_entry_lists,
)
from parsimony_sdmx.core.models import DatasetRecord


def _series_entries() -> list[Entity]:
    return [
        Entity(
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


def test_build_agency_dataset_entities_only_updates_manifested_flows() -> None:
    records = [
        DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield curve"),
        DatasetRecord(dataset_id="MIR", agency_id="ECB", title="Money market"),
    ]
    manifests = {
        dataset_code("ECB", "YC"): manifest_from_series_entries(_series_entries()),
    }

    entries = build_agency_dataset_entities(records, manifests)

    assert len(entries) == 1
    assert entries[0].code == "ECB|YC"
    assert entries[0].namespace == "sdmx_datasets_ecb"
    assert entries[0].metadata["dimensions"][0]["id"] == "FREQ"


def test_enrich_dataset_entities_preserves_entries_without_manifest() -> None:
    base = Entity(
        namespace="sdmx_datasets_ecb",
        code="ECB|MIR",
        title="Money market",
        metadata={"agency": "ECB", "dataset_id": "MIR"},
    )
    enriched = enrich_dataset_entities([base], {})

    assert enriched[0].metadata == base.metadata


def test_merge_dataset_entry_lists_upserts_by_code() -> None:
    existing = [
        Entity(
            namespace="sdmx_datasets_ecb",
            code="ECB|MIR",
            title="Old title",
            metadata={"agency": "ECB", "dataset_id": "MIR"},
        )
    ]
    updates = [
        Entity(
            namespace="sdmx_datasets_ecb",
            code="ECB|YC",
            title="Yield curve",
            metadata={"agency": "ECB", "dataset_id": "YC", "dimensions": [{"id": "FREQ", "values": []}]},
        )
    ]

    merged = merge_dataset_entry_lists(existing, updates)

    assert {(e.code, e.title) for e in merged} == {("ECB|MIR", "Old title"), ("ECB|YC", "Yield curve")}


def test_manifest_from_saved_series_reads_str_path() -> None:
    """Regression: resume batches pass save_root as str, not Path."""
    snapshot = Path("/tmp/parsimony-catalogs-v1/sdmx/sdmx_series_ecb_exr")
    if not (snapshot / "entries.parquet").is_file():
        pytest.skip("local ECB EXR snapshot not present")
    manifest = manifest_from_saved_series(str(snapshot))
    assert any(dim["id"] == "FREQ" for dim in manifest)
