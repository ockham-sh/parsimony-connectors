from parsimony.catalog import BM25Index, Entity, HybridIndex
from parsimony.ranking import ZScoreFusion

from parsimony_sdmx.catalog_policy import (
    HYBRID_BM25_WEIGHT,
    HYBRID_VECTOR_WEIGHT,
    derive_title_dimension_suffix,
    discover_dim_codes,
    sdmx_datasets_indexes,
    sdmx_dimension_manifest,
    sdmx_field_index,
    sdmx_series_entries,
    sdmx_series_indexes,
)


def _entries() -> list[Entity]:
    return [
        Entity(
            namespace="sdmx_series_ecb_yc",
            code="A.U2.SR_10Y",
            title="ECB source title",
            metadata={
                "FREQ_code": "A",
                "FREQ_label": "Annual",
                "REF_AREA_code": "U2",
                "REF_AREA_label": "Euro area",
                "DATA_TYPE_FM_code": "SR_10Y",
                "DATA_TYPE_FM_label": "Yield curve spot rate, 10-year maturity",
                "source_title": "ECB source title",
            },
        ),
        Entity(
            namespace="sdmx_series_ecb_yc",
            code="M.DE.IF_1Y",
            title="Synthetic title",
            metadata={
                "FREQ_code": "M",
                "FREQ_label": "Monthly",
                "REF_AREA_code": "DE",
                "REF_AREA_label": "Germany",
                "DATA_TYPE_FM_code": "IF_1Y",
                "DATA_TYPE_FM_label": "Instantaneous forward rate, 1-year maturity",
                "EXTRA_label": "Extra dimension label",
                "EXTRA_code": "X",
            },
        ),
    ]


def _assert_hybrid_index(index: HybridIndex) -> None:
    assert set(index._components) == {"bm25", "vector"}
    fusion = index._fusion
    assert isinstance(fusion, ZScoreFusion)
    assert fusion.weights["bm25"] == HYBRID_BM25_WEIGHT
    assert fusion.weights["vector"] == HYBRID_VECTOR_WEIGHT


def test_derive_title_dimension_suffix_adds_dimension_context() -> None:
    entry = _entries()[0]

    assert derive_title_dimension_suffix(entry) == (
        "FREQ: Annual; REF_AREA: Euro area; DATA_TYPE_FM: Yield curve spot rate, 10-year maturity"
    )


def test_derive_title_dimension_suffix_skips_missing_and_blank_labels() -> None:
    entry = Entity(
        namespace="ns",
        code="X",
        title="t",
        metadata={
            "FREQ_label": "Monthly",
            "REF_AREA_label": None,
            "CURRENCY_label": "",
            "ICP_ITEM_label": "Food",
        },
    )

    assert derive_title_dimension_suffix(entry) == "FREQ: Monthly; ICP_ITEM: Food"


def test_derive_title_dimension_suffix_ignores_non_label_fields() -> None:
    entry = Entity(
        namespace="ns",
        code="X",
        title="t",
        metadata={
            "FREQ_label": "Monthly",
            "FREQ_code": "M",
            "source_title": "Some title",
        },
    )

    assert derive_title_dimension_suffix(entry) == "FREQ: Monthly"


def test_sdmx_series_entries_attaches_index_strings_metadata() -> None:
    dims = ["FREQ", "REF_AREA", "DATA_TYPE_FM", "EXTRA"]
    augmented = sdmx_series_entries(_entries(), dims)

    assert len(augmented) == 2
    assert augmented[0].title == (
        "ECB source title | FREQ: Annual; REF_AREA: Euro area; DATA_TYPE_FM: Yield curve spot rate, 10-year maturity"
    )
    assert augmented[1].title == (
        "Synthetic title | FREQ: Monthly; REF_AREA: Germany; "
        "DATA_TYPE_FM: Instantaneous forward rate, 1-year maturity; "
        "EXTRA: Extra dimension label"
    )

    assert augmented[0].metadata["FREQ"] == "Annual"
    assert augmented[0].metadata["REF_AREA"] == "Euro area"
    assert augmented[1].metadata["EXTRA"] == "Extra dimension label"


def test_sdmx_field_index_cardinality() -> None:
    # Under 1000 unique values -> HybridIndex
    entries_under = [Entity(namespace="ns", code="x", title=f"title-{i}", metadata={}) for i in range(500)]
    idx_under = sdmx_field_index("title", entries_under)
    assert isinstance(idx_under, HybridIndex)
    _assert_hybrid_index(idx_under)

    # 1000 or more unique values -> BM25Index
    entries_over = [Entity(namespace="ns", code="x", title=f"title-{i}", metadata={}) for i in range(1005)]
    idx_over = sdmx_field_index("title", entries_over)
    assert isinstance(idx_over, BM25Index)


def test_sdmx_series_indexes_returns_field_keyed_hybrids() -> None:
    dims = ["FREQ", "REF_AREA"]
    augmented = sdmx_series_entries(_entries(), dims)
    indexes = sdmx_series_indexes(augmented, dims)

    assert set(indexes) == {"code", "title", "FREQ", "REF_AREA"}
    assert isinstance(indexes["code"], BM25Index)
    for field in ("title", "FREQ", "REF_AREA"):
        index = indexes[field]
        assert isinstance(index, HybridIndex)
        _assert_hybrid_index(index)


def test_sdmx_datasets_indexes_includes_code_bm25_and_title_hybrid() -> None:
    entries = [
        Entity(namespace="sdmx_datasets_ecb", code="ECB|YC", title="Yield curve", metadata={}),
    ]
    indexes = sdmx_datasets_indexes(entries)

    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    _assert_hybrid_index(indexes["title"])


def test_discover_dim_codes_returns_sorted_unique_ids() -> None:
    assert discover_dim_codes(_entries()) == ["DATA_TYPE_FM", "EXTRA", "FREQ", "REF_AREA"]


def test_sdmx_dimension_manifest_dedupes_and_caps_values() -> None:
    entries = _entries() + [
        Entity(
            namespace="sdmx_series_ecb_yc",
            code="M.DE.IF_2Y",
            title="Duplicate Germany monthly",
            metadata={
                "FREQ_code": "M",
                "FREQ_label": "Monthly",
                "REF_AREA_code": "DE",
                "REF_AREA_label": "Germany",
            },
        )
    ]
    dims = ["FREQ", "REF_AREA", "DATA_TYPE_FM"]
    manifest = sdmx_dimension_manifest(entries, dims, max_values_per_dimension=2)

    assert [item["id"] for item in manifest] == dims
    freq_values = manifest[0]["values"]
    assert freq_values == [{"code": "A", "label": "Annual"}, {"code": "M", "label": "Monthly"}]
    ref_values = manifest[1]["values"]
    assert ref_values == [{"code": "U2", "label": "Euro area"}, {"code": "DE", "label": "Germany"}]
    assert manifest[2]["values"] == [
        {"code": "SR_10Y", "label": "Yield curve spot rate, 10-year maturity"},
        {"code": "IF_1Y", "label": "Instantaneous forward rate, 1-year maturity"},
    ]


def test_sdmx_dimension_manifest_skips_blank_pairs() -> None:
    entry = Entity(
        namespace="ns",
        code="X",
        title="t",
        metadata={
            "FREQ_code": "M",
            "FREQ_label": "Monthly",
            "REF_AREA_code": "DE",
            "REF_AREA_label": "",
        },
    )
    manifest = sdmx_dimension_manifest([entry], ["FREQ", "REF_AREA"])

    assert manifest == [{"id": "FREQ", "values": [{"code": "M", "label": "Monthly"}]}, {"id": "REF_AREA", "values": []}]
