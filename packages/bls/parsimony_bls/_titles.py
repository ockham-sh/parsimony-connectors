"""Series-title resolution for catalog entries.

Most surveys' ``.series`` files carry a ready-made ``series_title``. For the few
that don't (SM/JT/PR), compose a searchable title by joining the resolved
dimension labels — the SDMX ``compose_series_title`` fallback, adapted to BLS.
"""

from __future__ import annotations

from parsimony_bls.flatfiles import dimension_columns, resolve_label


def compose_title(
    row: dict[str, str],
    columns: list[str],
    tables: dict[str, dict[str, str]],
) -> str:
    """Join a row's resolved dimension labels into a title (drops blanks/dupes)."""
    parts: list[str] = []
    seen: set[str] = set()
    for col in dimension_columns(columns):
        label = resolve_label(tables, col, row.get(col, "").strip()).strip()
        if label and label not in seen:
            seen.add(label)
            parts.append(label)
    return ", ".join(parts)


def title_for_row(
    row: dict[str, str],
    columns: list[str],
    tables: dict[str, dict[str, str]],
) -> str:
    """Best title for a series row: explicit ``series_title`` → composed → id."""
    explicit = row.get("series_title", "").strip()
    if explicit:
        return explicit
    composed = compose_title(row, columns, tables)
    return composed or row.get("series_id", "").strip()


__all__ = ["compose_title", "title_for_row"]
