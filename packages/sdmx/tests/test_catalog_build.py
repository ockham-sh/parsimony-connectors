"""Tests for SDMX dataset catalog assembly helpers."""

from __future__ import annotations

from parsimony.entity import Entity

from parsimony_sdmx.catalog_build import (
    dataset_code,
    dataset_entities_from_records,
    dataset_entity_from_structure,
    datasets_catalog,
    enrich_dataset_entities_with_dimensions,
    merge_dataset_entry_lists,
)
from parsimony_sdmx.core.models import (
    CodelistCode,
    CodelistRecord,
    DatasetRecord,
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


def test_dataset_entity_from_structure_includes_dimensions() -> None:
    entry = dataset_entity_from_structure(_structure())
    assert entry.code == "ECB|YC"
    assert entry.namespace == "sdmx_datasets_ecb"
    assert entry.metadata == {"dimensions": ["FREQ", "REF_AREA"]}


def test_enrich_dataset_entities_with_dimensions() -> None:
    base = Entity(
        namespace="sdmx_datasets_ecb",
        code=dataset_code("ECB", "YC"),
        title="Yield curve",
        metadata={"dimensions": []},
    )
    enriched = enrich_dataset_entities_with_dimensions([base], {dataset_code("ECB", "YC"): _structure()})
    assert enriched[0].metadata["dimensions"] == ["FREQ", "REF_AREA"]


def test_enrich_preserves_listing_title() -> None:
    """The listing title is authoritative (provider fallback for unnamed flows);
    DSD enrichment must never replace it with the structure fetch's name."""
    base = Entity(
        namespace="sdmx_datasets_ecb",
        code=dataset_code("ECB", "YC"),
        title="Yield curve (portal name)",
        metadata={"dimensions": []},
    )
    enriched = enrich_dataset_entities_with_dimensions([base], {dataset_code("ECB", "YC"): _structure()})
    assert enriched[0].title == "Yield curve (portal name)"


def test_merge_dataset_entry_lists_upserts_by_code() -> None:
    existing = [
        Entity(
            namespace="sdmx_datasets_ecb",
            code="ECB|MIR",
            title="Old title",
            metadata={"dimensions": []},
        )
    ]
    updates = [
        Entity(
            namespace="sdmx_datasets_ecb",
            code="ECB|YC",
            title="Yield curve",
            metadata={"dimensions": ["FREQ"]},
        )
    ]
    merged = merge_dataset_entry_lists(existing, updates)
    assert {(e.code, e.title) for e in merged} == {("ECB|MIR", "Old title"), ("ECB|YC", "Yield curve")}


def test_listing_entities_declare_dimensions_before_any_structure_fetch() -> None:
    """The listing carries no DSD, but the key must exist so the column always does."""
    entries = dataset_entities_from_records([DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield curve")])
    assert entries[0].metadata == {"dimensions": []}


def test_listing_entities_skip_derived_views() -> None:
    records = [
        DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield curve"),
        DatasetRecord(dataset_id="$DV_ABC", agency_id="ESTAT", title="Derived view"),
    ]
    assert [e.code for e in dataset_entities_from_records(records)] == ["ECB|YC"]


def test_datasets_catalog_namespace_follows_agency() -> None:
    assert datasets_catalog(agency="ECB").name == "sdmx_datasets_ecb"
