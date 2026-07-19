"""BoJ catalog indexing policy."""

from __future__ import annotations

from parsimony.catalog import BM25Index, HybridIndex, VectorIndex
from parsimony.catalog.policy import discovery_indexes
from parsimony.embedder import EmbeddingProvider, SentenceTransformerEmbedder

_DEFAULT_EMBEDDER: EmbeddingProvider | None = None


def _shared_embedder() -> EmbeddingProvider:
    global _DEFAULT_EMBEDDER
    if _DEFAULT_EMBEDDER is None:
        _DEFAULT_EMBEDDER = SentenceTransformerEmbedder()
    return _DEFAULT_EMBEDDER


def hybrid_index() -> HybridIndex:
    """BM25 + vector over a bounded discovery vocabulary (title, description)."""
    return HybridIndex(components=[BM25Index(), VectorIndex(embedder=_shared_embedder())])


__all__ = [
    "discovery_indexes",
    "hybrid_index",
]
