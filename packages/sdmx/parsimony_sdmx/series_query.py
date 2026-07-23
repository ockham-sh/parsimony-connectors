"""Map SDMX structured queries onto Catalog.search parameters."""

from __future__ import annotations

from dataclasses import dataclass

from parsimony.catalog import Catalog
from parsimony.catalog.query import StructuredQuery, parse_query
from parsimony.errors import InvalidParameterError

from parsimony_sdmx.series_fields import (
    TITLE_FIELD,
    parse_dim_from_field,
    searchable_fields,
    title_not_searchable_error,
)


@dataclass(frozen=True, slots=True)
class SeriesSearchPlan:
    """Resolved Catalog.search arguments for one SDMX series query."""

    query: str | None
    field: str | None
    filter: dict[str, list[str]]
    pinned_dims: frozenset[str]


def plan_series_search(
    query: str,
    *,
    catalog: Catalog,
    dsd_order: tuple[str, ...],
    top_k_per_dim: int,
) -> SeriesSearchPlan:
    """Translate SDMX ``field: value && …`` syntax into catalog search parameters."""
    # Only dimension fields can scope a query. `title` is admitted here purely so
    # a `title:` clause parses and can be answered with a reason — leaving it out
    # would surface the kernel's generic "field is not indexed" error instead.
    known = searchable_fields(dsd_order) | {TITLE_FIELD}
    parsed = parse_query(query, known_fields=known)
    if parsed is None:
        return SeriesSearchPlan(query=query.strip(), field=None, filter={}, pinned_dims=frozenset())

    if len(parsed.clauses) == 1:
        return _plan_single_clause(parsed)

    return _plan_multi_clause(parsed, catalog=catalog, top_k_per_dim=top_k_per_dim)


def _plan_single_clause(parsed: StructuredQuery) -> SeriesSearchPlan:
    field, values = parsed.clauses[0]
    dim = parse_dim_from_field(field)
    if dim is not None and dim[1] == "code":
        return SeriesSearchPlan(
            query=None,
            field=None,
            filter={field: list(values)},
            pinned_dims=frozenset({dim[0]}),
        )
    if field == TITLE_FIELD:
        raise title_not_searchable_error()
    # Every remaining admitted field is a dimension field.
    assert dim is not None, f"unreachable: unparseable clause field {field!r}"
    return SeriesSearchPlan(
        query=_join_values(values),
        field=field,
        filter={},
        pinned_dims=frozenset({dim[0]}),
    )


def _plan_multi_clause(
    parsed: StructuredQuery,
    *,
    catalog: Catalog,
    top_k_per_dim: int,
) -> SeriesSearchPlan:
    filter_spec: dict[str, list[str]] = {}
    pinned: set[str] = set()
    score_field: str | None = None
    score_query: str | None = None

    for clause_field, values in parsed.clauses:
        if clause_field == TITLE_FIELD:
            raise title_not_searchable_error()
        dim = parse_dim_from_field(clause_field)
        # Same invariant as the single-clause path.
        assert dim is not None, f"unreachable: unparseable clause field {clause_field!r}"
        dim_id, kind = dim
        pinned.add(dim_id)
        if kind == "code":
            filter_spec[clause_field] = list(values)
        elif kind == "label":
            resolved: list[str] = []
            for value in values:
                matches = catalog.search_values(value, clause_field, limit=top_k_per_dim)
                resolved.extend(match.value for match in matches)
            if resolved:
                filter_spec[clause_field] = resolved

    for clause_field, values in reversed(parsed.clauses):
        dim = parse_dim_from_field(clause_field)
        if dim is not None and dim[1] == "label":
            score_field = clause_field
            score_query = _join_values(values)
            break

    if score_field is None and score_query is None and not filter_spec:
        raise InvalidParameterError("sdmx", f"Could not plan catalog search for query {parsed!r}")

    return SeriesSearchPlan(
        query=score_query,
        field=score_field,
        filter=filter_spec,
        pinned_dims=frozenset(pinned),
    )


def _join_values(values: tuple[str, ...] | list[str]) -> str:
    return values[0] if len(values) == 1 else " ".join(values)


__all__ = ["SeriesSearchPlan", "plan_series_search"]
