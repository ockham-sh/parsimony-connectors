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

LABEL_SUFFIX = "_label"
CODE_SUFFIX = "_code"
DEFAULT_MAX_VALUES_PER_DIMENSION = 12

sdmx_field_index = adaptive_field_index


def _label_items(entry: Entity) -> list[tuple[str, str]]:
    return [
        (key.removesuffix(LABEL_SUFFIX), str(value).strip())
        for key, value in entry.metadata.items()
        if key.endswith(LABEL_SUFFIX) and value is not None and str(value).strip()
    ]


def derive_title_dimension_suffix(entry: Entity) -> str:
    """Return ``"dim: label; ..."`` summary of an entry's SDMX label fields.

    Used to augment series titles for hybrid (BM25 + vector) discovery; the
    suffix is concatenated onto ``entry.title`` in :func:`sdmx_series_entries`.
    Dimension names are the observed SDMX metadata keys without the
    ``_label`` suffix and serve as human-readable schema hints, not lookup
    codes.
    """

    return "; ".join(f"{name}: {value}" for name, value in _label_items(entry))


def sdmx_series_entries(entries: Sequence[Entity], dim_codes: list[str]) -> list[Entity]:
    """Return *entries* augmented with direct per-dimension metadata keys and composite title."""

    out: list[Entity] = []
    for entry in entries:
        metadata = dict(entry.metadata)

        for dim in dim_codes:
            lbl_key = f"{dim}_label"
            if lbl_key in entry.metadata:
                metadata[dim] = entry.metadata[lbl_key]

        composite = derive_title_dimension_suffix(entry)

        augmented_title = entry.title
        if composite and composite not in entry.title:
            augmented_title = f"{entry.title} | {composite}"

        out.append(
            Entity(
                namespace=entry.namespace,
                code=entry.code,
                title=augmented_title,
                metadata=metadata,
            )
        )
    return out


def sdmx_series_indexes(
    entries: Sequence[Entity],
    dim_codes: list[str],
) -> dict[str, CatalogIndex]:
    """Return SDMX series indexes: hybrid or BM25 per field based on cardinality."""

    indexes: dict[str, CatalogIndex] = {
        "code": BM25Index(),
        "title": sdmx_field_index("title", entries),
    }
    for dim in dim_codes:
        indexes[dim] = sdmx_field_index(dim, entries)
    return indexes


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


def discover_dim_codes(entries: Sequence[Entity]) -> list[str]:
    """Return sorted SDMX dimension IDs observed in series entry metadata."""

    dim_codes: set[str] = set()
    for entry in entries:
        for key in entry.metadata:
            if key.endswith(LABEL_SUFFIX):
                dim_codes.add(key.removesuffix(LABEL_SUFFIX))
    return sorted(dim_codes)


def sdmx_dimension_manifest(
    entries: Sequence[Entity],
    dim_codes: list[str],
    *,
    max_values_per_dimension: int = DEFAULT_MAX_VALUES_PER_DIMENSION,
) -> list[dict[str, Any]]:
    """Build a compact, JSON-serializable manifest of searchable SDMX dimensions.

    Each dimension lists up to *max_values_per_dimension* distinct
    ``(code, label)`` pairs in stable first-seen order. Blank codes or labels
    are skipped.
    """

    if max_values_per_dimension < 1:
        raise ValueError("max_values_per_dimension must be >= 1")

    manifest: list[dict[str, Any]] = []
    for dim in dim_codes:
        code_key = f"{dim}{CODE_SUFFIX}"
        label_key = f"{dim}{LABEL_SUFFIX}"
        seen: set[tuple[str, str]] = set()
        values: list[dict[str, str]] = []
        for entry in entries:
            raw_code = entry.metadata.get(code_key)
            raw_label = entry.metadata.get(label_key)
            code = str(raw_code).strip() if raw_code is not None else ""
            label = str(raw_label).strip() if raw_label is not None else ""
            if not code or not label:
                continue
            pair = (code, label)
            if pair in seen:
                continue
            seen.add(pair)
            values.append({"code": code, "label": label})
            if len(values) >= max_values_per_dimension:
                break
        manifest.append({"id": dim, "values": values})
    return manifest


__all__ = [
    "DEFAULT_MAX_VALUES_PER_DIMENSION",
    "HYBRID_BM25_WEIGHT",
    "HYBRID_UNIQUE_VALUE_LIMIT",
    "HYBRID_VECTOR_WEIGHT",
    "derive_title_dimension_suffix",
    "discover_dim_codes",
    "sdmx_dimension_manifest",
    "sdmx_datasets_indexes",
    "sdmx_field_index",
    "sdmx_series_entries",
    "sdmx_series_indexes",
]
