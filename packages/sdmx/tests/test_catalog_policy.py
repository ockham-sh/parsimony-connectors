from parsimony.catalog import BM25Index, HybridIndex
from parsimony.ranking import ZScoreFusion

from parsimony_sdmx.catalog_policy import (
    HYBRID_BM25_WEIGHT,
    HYBRID_VECTOR_WEIGHT,
    sdmx_datasets_indexes,
    sdmx_title_index,
)


def _assert_hybrid_index(index: HybridIndex) -> None:
    assert set(index._components) == {"bm25", "vector"}
    fusion = index._fusion
    assert isinstance(fusion, ZScoreFusion)
    assert fusion.weights["bm25"] == HYBRID_BM25_WEIGHT
    assert fusion.weights["vector"] == HYBRID_VECTOR_WEIGHT


def test_sdmx_title_index_is_hybrid() -> None:
    index = sdmx_title_index()
    assert isinstance(index, HybridIndex)
    _assert_hybrid_index(index)


def test_sdmx_datasets_indexes_includes_code_bm25_and_title_hybrid() -> None:
    indexes = sdmx_datasets_indexes()

    # Titles only carry the semantic load — no description index (DSD-vocab
    # text matches flows that break down BY a subject, not flows ABOUT it).
    assert set(indexes) == {"code", "title"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    _assert_hybrid_index(indexes["title"])
