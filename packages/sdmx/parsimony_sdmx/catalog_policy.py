"""SDMX-specific catalog indexing policy."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from parsimony.catalog import BM25Index, CatalogIndex, Entity
from parsimony.catalog.policy import (
    HYBRID_BM25_WEIGHT,
    HYBRID_UNIQUE_VALUE_LIMIT,
    HYBRID_VECTOR_WEIGHT,
    adaptive_field_index,
)

sdmx_field_index = adaptive_field_index


def sdmx_datasets_indexes(
    entries: Sequence[Entity],
) -> dict[str, CatalogIndex]:
    """Return per-agency dataset catalog indexes for keyword/semantic discovery.

    Includes a lexical ``code`` index so agents can retrieve a dataset by its
    composite ``{agency}|{dataset_id}`` key (e.g. ``ECB|YC``); title and
    description carry the semantic load via hybrid (BM25 + vector) indexes or BM25 only.
    """

    return {
        "code": BM25Index(),
        "title": sdmx_field_index("title", entries),
        "description": sdmx_field_index("description", entries),
    }


def dsd_summary_from_structure(record: Any) -> list[dict[str, Any]]:
    """Build JSON-serializable DSD summary for dataset catalog metadata."""
    summary: list[dict[str, Any]] = []
    for dim in record.dimensions:
        summary.append(
            {
                "dimension_id": dim.dimension_id,
                "name": dim.name or dim.dimension_id,
                "codelist_id": dim.codelist_id,
                "code_count": dim.code_count,
                "sample": [{"code": sample.code, "label": sample.label} for sample in dim.sample],
            }
        )
    return summary


def dsd_description_text(record: Any) -> str:
    """Compact vocabulary text folded into dataset description for recall."""
    parts: list[str] = []
    for dim in record.dimensions:
        dim_bits = [dim.name or dim.dimension_id]
        if dim.codelist_id:
            dim_bits.append(f"codelist={dim.codelist_id}")
        if dim.sample:
            labels = ", ".join(f"{s.label}" for s in dim.sample[:3])
            dim_bits.append(f"examples: {labels}")
        parts.append("; ".join(dim_bits))
    return " | ".join(parts)


__all__ = [
    "HYBRID_BM25_WEIGHT",
    "HYBRID_UNIQUE_VALUE_LIMIT",
    "HYBRID_VECTOR_WEIGHT",
    "dsd_description_text",
    "dsd_summary_from_structure",
    "sdmx_field_index",
    "sdmx_datasets_indexes",
]
