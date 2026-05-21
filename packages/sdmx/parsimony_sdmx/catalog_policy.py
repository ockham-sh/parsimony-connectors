"""SDMX-specific catalog indexing policy."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from parsimony.catalog import BM25Index, CatalogEntry, CatalogIndex, HybridIndex, VectorIndex
from parsimony.ranking import ZScoreFusion

LABEL_SUFFIX = "_label"
CODE_SUFFIX = "_code"
DEFAULT_MAX_VALUES_PER_DIMENSION = 12
HYBRID_UNIQUE_VALUE_LIMIT = 100_000
HYBRID_BM25_WEIGHT = 0.5
HYBRID_VECTOR_WEIGHT = 1.0


def _catalog_field_text(entry: CatalogEntry, field: str) -> str:
    """Mirror :func:`parsimony.catalog._field_text` for build-time cardinality checks."""

    if field == "namespace":
        return entry.namespace
    if field == "code":
        return entry.code
    if field == "title":
        return entry.title
    value = entry.metadata.get(field)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return " ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return " ".join(f"{key}: {item}" for key, item in value.items() if item is not None)
    return str(value)


def unique_nonempty_field_text_count(entries: Sequence[CatalogEntry], field: str) -> int:
    """Count distinct non-empty texts for *field* (matches vector-index deduplication input)."""

    texts: set[str] = set()
    for entry in entries:
        text = _catalog_field_text(entry, field).strip()
        if text:
            texts.add(text)
    return len(texts)


def sdmx_field_index(
    field: str,
    entries: Sequence[CatalogEntry],
    *,
    unique_value_limit: int = HYBRID_UNIQUE_VALUE_LIMIT,
    bm25_weight: float = HYBRID_BM25_WEIGHT,
    vector_weight: float = HYBRID_VECTOR_WEIGHT,
) -> CatalogIndex:
    """Return a hybrid BM25+vector index or BM25-only, based on unique field text cardinality."""

    if unique_value_limit < 1:
        raise ValueError("unique_value_limit must be >= 1")

    prefix = field.lower()
    bm25_name = f"{prefix}_bm25"
    vector_name = f"{prefix}_vector"
    unique_count = unique_nonempty_field_text_count(entries, field)
    if 0 < unique_count < unique_value_limit:
        return HybridIndex(
            f"{prefix}_hybrid",
            field=field,
            indexes=[
                BM25Index(bm25_name, field=field),
                VectorIndex(vector_name, field=field),
            ],
            fusion=ZScoreFusion(weights={bm25_name: bm25_weight, vector_name: vector_weight}),
        )
    return BM25Index(bm25_name, field=field)


def _label_items(entry: CatalogEntry) -> list[tuple[str, str]]:
    return [
        (key.removesuffix(LABEL_SUFFIX), str(value).strip())
        for key, value in entry.metadata.items()
        if key.endswith(LABEL_SUFFIX) and value is not None and str(value).strip()
    ]


def derive_title_dimension_suffix(entry: CatalogEntry) -> str:
    """Return ``"dim: label; ..."`` summary of an entry's SDMX label fields.

    Used to augment series titles for hybrid (BM25 + vector) discovery; the
    suffix is concatenated onto ``entry.title`` in :func:`sdmx_series_entries`.
    Dimension names are the observed SDMX metadata keys without the
    ``_label`` suffix and serve as human-readable schema hints, not lookup
    codes.
    """

    return "; ".join(f"{name}: {value}" for name, value in _label_items(entry))


def sdmx_series_entries(entries: Sequence[CatalogEntry], dim_codes: list[str]) -> list[CatalogEntry]:
    """Return *entries* augmented with direct per-dimension metadata keys and composite title."""

    out: list[CatalogEntry] = []
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
            CatalogEntry(
                namespace=entry.namespace,
                code=entry.code,
                title=augmented_title,
                metadata=metadata,
            )
        )
    return out


def sdmx_series_indexes(
    entries: Sequence[CatalogEntry],
    dim_codes: list[str],
    *,
    unique_value_limit: int = HYBRID_UNIQUE_VALUE_LIMIT,
) -> list[CatalogIndex]:
    """Return SDMX series indexes: hybrid or BM25 per field from entry cardinality."""

    indexes: list[CatalogIndex] = [
        sdmx_field_index("title", entries, unique_value_limit=unique_value_limit),
    ]
    for dim in dim_codes:
        indexes.append(sdmx_field_index(dim, entries, unique_value_limit=unique_value_limit))
    return indexes


def sdmx_datasets_indexes(
    entries: Sequence[CatalogEntry],
    *,
    unique_value_limit: int = HYBRID_UNIQUE_VALUE_LIMIT,
) -> list[CatalogIndex]:
    """Return cross-agency dataset catalog indexes for keyword/semantic discovery.

    Includes a lexical ``code`` index so agents can retrieve a dataset by its
    composite ``{agency}|{dataset_id}`` key (e.g. ``ECB|YC``); title and
    description carry the semantic load via hybrid (BM25 + vector) indexes
    when cardinality is below the policy limit, falling back to BM25 only.
    """

    return [
        BM25Index("code_bm25", field="code"),
        sdmx_field_index("title", entries, unique_value_limit=unique_value_limit),
        sdmx_field_index("description", entries, unique_value_limit=unique_value_limit),
    ]


def discover_dim_codes(entries: Sequence[CatalogEntry]) -> list[str]:
    """Return sorted SDMX dimension IDs observed in series entry metadata."""

    dim_codes: set[str] = set()
    for entry in entries:
        for key in entry.metadata:
            if key.endswith(LABEL_SUFFIX):
                dim_codes.add(key.removesuffix(LABEL_SUFFIX))
    return sorted(dim_codes)


def sdmx_dimension_manifest(
    entries: Sequence[CatalogEntry],
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
    "unique_nonempty_field_text_count",
]
