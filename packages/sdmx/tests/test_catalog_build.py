"""Tests for SDMX dataset catalog assembly helpers."""

from __future__ import annotations

from parsimony.entity import Entity

from parsimony_sdmx.catalog_build import (
    dataset_code,
    dataset_entity_from_structure,
    enrich_dataset_entities_with_dsd,
    merge_dataset_entry_lists,
)
from parsimony_sdmx.core.models import (
    CodelistCode,
    CodelistRecord,
    DimensionStructure,
    StructureRecord,
)


def _structure(dataset_id: str = "YC", cl_id: str = "CL_FREQ") -> StructureRecord:
    return StructureRecord(
        dataset_id=dataset_id,
        agency_id="ECB",
        title="Yield curve",
        dsd_order=("FREQ", "REF_AREA"),
        dimensions=(
            DimensionStructure(
                dimension_id="FREQ",
                codelist_id=cl_id,
                name="FREQ",
                code_count=2,
                sample=(CodelistCode(code="M", label="Monthly"),),
            ),
        ),
        codelists=(
            CodelistRecord(
                codelist_id=cl_id,
                codes=(CodelistCode(code="M", label="Monthly"), CodelistCode(code="A", label="Annual")),
            ),
        ),
    )


def test_dataset_entity_from_structure_includes_dsd() -> None:
    entry = dataset_entity_from_structure(_structure())
    assert entry.code == "ECB|YC"
    assert entry.namespace == "sdmx_datasets_ecb"
    assert entry.metadata["dsd"][0]["dimension_id"] == "FREQ"
    assert entry.metadata["dsd"][0]["codelist_id"] == "CL_FREQ"
    assert "description" not in entry.metadata


def test_enrich_dataset_entities_with_dsd() -> None:
    base = Entity(
        namespace="sdmx_datasets_ecb",
        code=dataset_code("ECB", "YC"),
        title="Yield curve",
        metadata={"agency": "ECB", "dataset_id": "YC"},
    )
    enriched = enrich_dataset_entities_with_dsd([base], {dataset_code("ECB", "YC"): _structure()})
    assert enriched[0].metadata["dsd"][0]["codelist_id"] == "CL_FREQ"


def test_enrich_preserves_listing_title() -> None:
    """The listing title is authoritative (provider fallback for unnamed flows);
    DSD enrichment must never replace it with the structure fetch's name."""
    base = Entity(
        namespace="sdmx_datasets_ecb",
        code=dataset_code("ECB", "YC"),
        title="Yield curve (portal name)",
        metadata={"agency": "ECB", "dataset_id": "YC"},
    )
    enriched = enrich_dataset_entities_with_dsd([base], {dataset_code("ECB", "YC"): _structure()})
    assert enriched[0].title == "Yield curve (portal name)"


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
            metadata={"agency": "ECB", "dataset_id": "YC", "dsd": [{"dimension_id": "FREQ", "values": []}]},
        )
    ]
    merged = merge_dataset_entry_lists(existing, updates)
    assert {(e.code, e.title) for e in merged} == {("ECB|MIR", "Old title"), ("ECB|YC", "Yield curve")}
