"""SDMX series catalog field naming helpers."""

from __future__ import annotations

from parsimony.errors import InvalidParameterError

SERIES_PARQUET = "series.parquet"
META_FILENAME = "meta.json"
TITLE_FIELD = "title"


def title_not_searchable_error() -> InvalidParameterError:
    """The single explanation for every route that tries to search ``title``.

    ``fields="title"`` and a ``title:`` query clause are the same mistake, so they
    get the same answer. Without this, the clause route surfaces the kernel's
    generic "field is not indexed" message, which is true but does not say that
    title is *deliberately* unindexed or what to do instead.
    """
    return InvalidParameterError(
        "sdmx",
        "title is not searchable on a series catalog: it is composed from the "
        "dimension labels at build time, so it carries nothing the {dim}_label "
        "indexes do not already hold, and it is a display column only. Use a bare "
        "query to rank against every dimension label, or scope to a {dim}_label "
        "field. Filtering on title via filter_json still works.",
    )


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
    """Every real column of a series catalog — the valid ``filter_json`` keys.

    Includes ``title``: it is a genuine parquet column and filtering on it is a
    pyarrow operation that needs no index. For what a *query* may be scored
    against, use :func:`searchable_fields` instead — those are not the same set.
    """
    fields = {TITLE_FIELD}
    for dim in dsd_order:
        fields.add(dim_code_field(dim))
        fields.add(dim_label_field(dim))
    return fields


def searchable_fields(dsd_order: tuple[str, ...] | list[str]) -> set[str]:
    """Columns a query can be scored against — every column except ``title``.

    ``title`` is composed at build time by concatenating the same dimension
    labels the ``{dim}_label`` indexes already carry, so it holds no information
    they lack; scoring it only re-counts matched terms. It is a display column,
    never a search surface.
    """
    fields: set[str] = set()
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
    "searchable_fields",
    "title_not_searchable_error",
]
