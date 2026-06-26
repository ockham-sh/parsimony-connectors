from parsimony.catalog import BM25Index, Entity, HybridIndex
from parsimony.ranking import ZScoreFusion

from parsimony_sdmx.catalog_policy import (
    HYBRID_BM25_WEIGHT,
    HYBRID_VECTOR_WEIGHT,
    sdmx_datasets_indexes,
    sdmx_field_index,
)


def _assert_hybrid_index(index: HybridIndex) -> None:
    assert set(index._components) == {"bm25", "vector"}
    fusion = index._fusion
    assert isinstance(fusion, ZScoreFusion)
    assert fusion.weights["bm25"] == HYBRID_BM25_WEIGHT
    assert fusion.weights["vector"] == HYBRID_VECTOR_WEIGHT


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


def test_sdmx_datasets_indexes_includes_code_bm25_and_title_hybrid() -> None:
    entries = [
        Entity(namespace="sdmx_datasets_ecb", code="ECB|YC", title="Yield curve", metadata={}),
    ]
    indexes = sdmx_datasets_indexes(entries)

    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    _assert_hybrid_index(indexes["title"])
