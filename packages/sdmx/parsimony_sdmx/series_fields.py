"""SDMX series catalog field naming helpers."""

from __future__ import annotations

SERIES_PARQUET = "series.parquet"
META_FILENAME = "meta.json"
TITLE_FIELD = "title"


def dim_code_field(dim_id: str) -> str:
    """Indexed column / filter field for a dimension code."""
    return f"{dim_id}_code"


def dim_label_field(dim_id: str) -> str:
    """Indexed column / search field for a dimension label."""
    return f"{dim_id}_label"


def parse_dim_from_field(field: str) -> tuple[str, str] | None:
    """Return ``(dim_id, kind)`` where kind is ``code`` or ``label``, else None."""
    if field == TITLE_FIELD:
        return None
    if field.endswith("_label"):
        return field[: -len("_label")], "label"
    if field.endswith("_code"):
        return field[: -len("_code")], "code"
    return None


def known_search_fields(dsd_order: tuple[str, ...] | list[str]) -> set[str]:
    fields = {TITLE_FIELD}
    for dim in dsd_order:
        fields.add(dim_code_field(dim))
        fields.add(dim_label_field(dim))
    return fields


__all__ = [
    "META_FILENAME",
    "SERIES_PARQUET",
    "TITLE_FIELD",
    "dim_code_field",
    "dim_label_field",
    "known_search_fields",
    "parse_dim_from_field",
]
