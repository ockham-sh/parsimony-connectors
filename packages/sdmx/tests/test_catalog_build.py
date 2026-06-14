"""Tests for SDMX DSD/codelist catalog assembly helpers."""

from __future__ import annotations

import pytest
from parsimony.entity import Entity

from parsimony_sdmx.catalog_build import (
    accumulate_codelists,
    assert_codelist_namespace_unique,
    codelist_entities,
    dataset_code,
    dataset_entity_from_structure,
    enrich_dataset_entities_with_dsd,
    merge_codelist_records,
    merge_dataset_entry_lists,
)
from parsimony_sdmx.connectors.codelist_namespace import codelist_namespace
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
    assert entry.metadata["dsd"][0]["codelist_namespace"] == codelist_namespace("ECB", "CL_FREQ")
    assert "Monthly" in entry.metadata["description"]


def test_codelist_entities_use_label_as_title() -> None:
    entries = codelist_entities(_structure().codelists[0], agency="ECB")
    assert entries[0].code == "M"
    assert entries[0].title == "Monthly"
    assert entries[0].namespace == codelist_namespace("ECB", "CL_FREQ")


def test_merge_codelist_records_dedups_codes() -> None:
    a = CodelistRecord("CL_GEO", (CodelistCode("DE", "Germany"),))
    b = CodelistRecord("CL_GEO", (CodelistCode("FR", "France"), CodelistCode("DE", "Germany")))
    merged = merge_codelist_records(a, b)
    assert {c.code for c in merged.codes} == {"DE", "FR"}


def test_accumulate_codelists_merges_across_flows() -> None:
    bucket: dict[str, CodelistRecord] = {}
    accumulate_codelists(bucket, _structure("YC", "CL_FREQ"))
    accumulate_codelists(bucket, _structure("HICP", "CL_FREQ"))
    assert len(bucket) == 1
    assert len(bucket["CL_FREQ"].codes) == 2


def test_assert_codelist_namespace_unique_detects_collision() -> None:
    bucket = {
        "CL.FREQ": CodelistRecord("CL.FREQ", (CodelistCode("M", "Monthly"),)),
        "CL_FREQ": CodelistRecord("CL_FREQ", (CodelistCode("A", "Annual"),)),
    }
    with pytest.raises(ValueError, match="collision"):
        assert_codelist_namespace_unique(bucket, agency="ECB")


def test_enrich_dataset_entities_with_dsd() -> None:
    base = Entity(
        namespace="sdmx_datasets_ecb",
        code=dataset_code("ECB", "YC"),
        title="Yield curve",
        metadata={"agency": "ECB", "dataset_id": "YC"},
    )
    enriched = enrich_dataset_entities_with_dsd([base], {dataset_code("ECB", "YC"): _structure()})
    assert enriched[0].metadata["dsd"][0]["codelist_id"] == "CL_FREQ"


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
