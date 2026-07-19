"""BLS catalog indexing policy + dimension-manifest derivation.

Adapted from the SDMX policy: per-survey series catalogs carry one index per
dimension plus title/code, and the tier-1 survey catalog gets a compact dimension
manifest derived from a survey's series entries (codes + labels), so an agent can
navigate dimensions or construct a series id.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from parsimony.catalog import BM25Index, CatalogIndex, Entity, HybridIndex, VectorIndex
from parsimony.embedder import EmbeddingProvider, SentenceTransformerEmbedder

LABEL_SUFFIX = "_label"
CODE_SUFFIX = "_code"
DEFAULT_MAX_VALUES_PER_DIMENSION = 12

_DEFAULT_EMBEDDER: EmbeddingProvider | None = None


def _shared_embedder() -> EmbeddingProvider:
    global _DEFAULT_EMBEDDER
    if _DEFAULT_EMBEDDER is None:
        _DEFAULT_EMBEDDER = SentenceTransformerEmbedder()
    return _DEFAULT_EMBEDDER


def _hybrid_index() -> HybridIndex:
    """BM25 + vector over a bounded discovery/label vocabulary."""
    return HybridIndex(components=[BM25Index(), VectorIndex(embedder=_shared_embedder())])


def discover_dim_codes(entries: Sequence[Entity]) -> list[str]:
    """Sorted dimension ids observed in series-entry metadata (from ``<dim>_label``)."""
    dims: set[str] = set()
    for entry in entries:
        for key in entry.metadata:
            if key.endswith(LABEL_SUFFIX):
                dims.add(key.removesuffix(LABEL_SUFFIX))
    return sorted(dims)


def series_entries(entries: Sequence[Entity], dim_codes: list[str]) -> list[Entity]:
    """Augment series entries with a direct ``<dim>`` (label) key per dimension.

    The label-valued ``<dim>`` keys are what the per-dimension indexes search, so a
    structured clause like ``area: "U.S. city average"`` resolves.
    """
    out: list[Entity] = []
    for entry in entries:
        metadata = dict(entry.metadata)
        for dim in dim_codes:
            label = entry.metadata.get(f"{dim}{LABEL_SUFFIX}")
            if label is not None:
                metadata[dim] = label
        out.append(
            Entity(
                namespace=entry.namespace,
                code=entry.code,
                title=entry.title,
                metadata=metadata,
            )
        )
    return out


def series_indexes(entries: Sequence[Entity], dim_codes: list[str]) -> dict[str, CatalogIndex]:
    """Per-survey series indexes, index kind by field role.

    ``code`` is a series identifier and ``title`` is row-composed text — both
    have as many distinct values as there are series, so BM25 alone (vector
    semantics over per-row strings buy nothing). Each ``<dim>`` field holds a
    bounded label vocabulary (area names, item names, …), so it carries a
    vector component for semantic label matching. *entries* is kept for
    call-site compatibility; the role policy does not inspect it.
    """
    del entries
    indexes: dict[str, CatalogIndex] = {
        "code": BM25Index(),
        "title": BM25Index(),
    }
    for dim in dim_codes:
        indexes[dim] = _hybrid_index()
    return indexes


def surveys_indexes(entries: Sequence[Entity]) -> dict[str, CatalogIndex]:
    """Tier-1 survey catalog indexes: BM25 code (survey abbrev) + hybrid title.

    The survey catalog is a small discovery catalog — one bounded title per
    survey — so the title carries a vector component; the code (survey abbrev)
    is an identifier, BM25 only. *entries* is kept for call-site compatibility.
    """
    del entries
    return {
        "code": BM25Index(),
        "title": _hybrid_index(),
    }


def manifest_from_series_entries(
    entries: Sequence[Entity],
    *,
    max_values_per_dimension: int = DEFAULT_MAX_VALUES_PER_DIMENSION,
) -> list[dict[str, Any]]:
    """Compact ``[{id, values:[{code,label}…]}]`` manifest from series entries."""
    dim_codes = discover_dim_codes(entries)
    manifest: list[dict[str, Any]] = []
    for dim in dim_codes:
        code_key = f"{dim}{CODE_SUFFIX}"
        label_key = f"{dim}{LABEL_SUFFIX}"
        seen: set[str] = set()
        values: list[dict[str, str]] = []
        for entry in entries:
            raw_code = entry.metadata.get(code_key)
            code = str(raw_code).strip() if raw_code is not None else ""
            if not code or code in seen:
                continue
            seen.add(code)
            raw_label = entry.metadata.get(label_key)
            label = str(raw_label).strip() if raw_label is not None else ""
            values.append({"code": code, "label": label or code})
            if len(values) >= max_values_per_dimension:
                break
        manifest.append({"id": dim, "values": values})
    return manifest


__all__ = [
    "DEFAULT_MAX_VALUES_PER_DIMENSION",
    "discover_dim_codes",
    "manifest_from_series_entries",
    "series_entries",
    "series_indexes",
    "surveys_indexes",
]
