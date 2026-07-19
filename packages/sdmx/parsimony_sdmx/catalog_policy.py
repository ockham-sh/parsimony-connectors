"""SDMX-specific catalog indexing policy."""

from __future__ import annotations

from parsimony.catalog import BM25Index, CatalogIndex, HybridIndex, VectorIndex
from parsimony.embedder import EmbeddingProvider, SentenceTransformerEmbedder


def sdmx_title_index(embedder: EmbeddingProvider | None = None) -> HybridIndex:
    """Hybrid (BM25 + vector) index over dataset titles, for every agency.

    Embedding cost scales with the number of *unique* values in the field, and a
    dataset catalog holds one title per flow — thousands at worst, never the
    millions that make embedding expensive. Sizing this per agency only made
    semantics depend on how many flows an agency happens to publish: Eurostat's
    7.7k titles lost semantic matching entirely while the ECB's 104 kept it.
    """
    return HybridIndex(
        components=[BM25Index(), VectorIndex(embedder=embedder or SentenceTransformerEmbedder())],
    )


def sdmx_datasets_indexes(embedder: EmbeddingProvider | None = None) -> dict[str, CatalogIndex]:
    """Return per-agency dataset catalog indexes for keyword/semantic discovery.

    Includes a lexical ``code`` index so agents can retrieve a dataset by its
    composite ``{agency}|{dataset_id}`` key (e.g. ``ECB|YC``); the title carries
    the semantic load. No description index: DSD-vocabulary text matches flows
    that break down *by* a subject, not flows *about* it, and outranks genuine
    title hits.
    """

    return {
        "code": BM25Index(),
        "title": sdmx_title_index(embedder),
    }


__all__ = [
    "sdmx_datasets_indexes",
    "sdmx_title_index",
]
