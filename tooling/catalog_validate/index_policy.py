"""Adaptive indexing helpers for official connector catalog builds.

Re-exports core policy; see :mod:`parsimony.catalog.policy`.
"""

from __future__ import annotations

from parsimony.catalog.policy import (
    HYBRID_BM25_WEIGHT,
    HYBRID_UNIQUE_VALUE_LIMIT,
    HYBRID_VECTOR_WEIGHT,
    adaptive_field_index,
    discovery_indexes,
)

__all__ = [
    "HYBRID_BM25_WEIGHT",
    "HYBRID_UNIQUE_VALUE_LIMIT",
    "HYBRID_VECTOR_WEIGHT",
    "adaptive_field_index",
    "discovery_indexes",
]
