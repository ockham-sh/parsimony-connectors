"""``sdmx_series_search`` — columnar per-flow series discovery from local catalogs."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from functools import lru_cache
from itertools import islice
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import pyarrow.dataset as ds
from parsimony.catalog import Catalog, SearchDetail, resolve_catalog_dir
from parsimony.catalog.filters import AllOf, FieldIn, Filter, FilterLike, as_filter
from parsimony.catalog.search import RANKING_COLUMNS, resolved_catalog_url, wire_score, wire_search_detail
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.catalog.storage import read_meta
from parsimony.connector import connector
from parsimony.errors import CatalogNotFoundError, ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputSpec
from pydantic import BaseModel, Field

from parsimony_sdmx.connectors.datasets_search import DEFAULT_CATALOG_ROOT, PARSIMONY_SDMX_CATALOG_URL_ENV
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.core.namespaces import series_namespace
from parsimony_sdmx.series_fields import (
    SERIES_PARQUET,
    TITLE_FIELD,
    dim_code_field,
    dim_label_field,
    known_search_fields,
    parse_dim_from_field,
)

logger = logging.getLogger(__name__)

DEFAULT_LRU_SIZE = 4

#: A free-text ``query`` is a ranked shortlist for reading — capped small. A pure
#: ``filter`` lookup is an *enumeration* of the already-cached local catalog into a
#: kernel variable (the agent filters/charts it in-sandbox), so it may return a whole
#: dimension slice — the field report's 574-series slice exceeded the old 500 ceiling.
RANKED_LIMIT = 500
ENUMERATION_LIMIT = 10_000


_CATALOG_LRU_ENV_VAR = "PARSIMONY_SDMX_CATALOG_LRU_SIZE"


def _lru_size_from_env() -> int:
    raw = os.environ.get(_CATALOG_LRU_ENV_VAR, "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


@lru_cache(maxsize=_lru_size_from_env())
def _load_series_catalog(namespace: str, catalog_path: str) -> Catalog:
    return Catalog.load(f"file://{catalog_path}")


def _clear_series_catalog_lru() -> None:
    _load_series_catalog.cache_clear()


def _not_published(label: str) -> str:
    """The single "this flow has no published series catalog" message, shared by every caller.

    "Not published" means not in the *parsimony* catalog — it says nothing about whether
    the flow exists upstream at the agency, and the message must not conflate the two: a
    caller hunting a successor flow (e.g. ECB's post-BPM6 BOP) needs to know the id may
    still be real.
    """
    return (
        f"No series catalog for {label}: this flow is not published in the parsimony catalog "
        "(it may still exist upstream at the agency). Verify the flow id with "
        "sdmx_datasets_search; if it is real, ask the maintainers to build its catalog."
    )


def _resolve_catalog_path(namespace: str, *, label: str, catalog_root: str | None = None) -> Path:
    """Resolve this flow's catalog to a local directory (for parquet + Catalog.load).

    Delegates URL resolution to the framework: ``resolve_catalog_dir`` handles
    every scheme (``file://`` and ``hf://``) and, for a sub-path ``hf://`` catalog,
    downloads only this flow's sub-tree rather than enumerating the whole SDMX
    monorepo. The connector holds no scheme knowledge of its own.

    A flow that was never built has no sub-tree on the remote (an ``hf://`` 404 →
    ``EntryNotFoundError``) or an empty one (``CatalogNotFoundError``); both mean the same
    thing, so translate them into the one friendly "not published" message rather than
    leaking a raw Hugging Face 404. A genuine network failure is a *different* exception
    and propagates as-is — an unreachable Hub is not "not published."
    """
    from huggingface_hub.errors import EntryNotFoundError

    root = resolved_catalog_url(
        PARSIMONY_SDMX_CATALOG_URL_ENV,
        DEFAULT_CATALOG_ROOT,
        override=catalog_root,
    )
    cache_path = Path(lazy_catalog_dir("sdmx", namespace))
    if cache_path.is_dir():
        return cache_path
    try:
        return resolve_catalog_dir(f"{root}/{namespace}")
    except ValueError as exc:
        # resolve_catalog_dir raises ValueError for an unsupported scheme; keep the
        # connector's structured error type so callers catching ConnectorError see it.
        raise ConnectorError(str(exc), provider="sdmx") from exc
    except (EntryNotFoundError, CatalogNotFoundError) as exc:
        raise ConnectorError(_not_published(label), provider="sdmx") from exc


def _parse_agency(agency: str) -> AgencyId:
    raw = agency.strip().upper()
    if not raw:
        raise InvalidParameterError("sdmx", "agency must be non-empty")
    try:
        return AgencyId(raw)
    except ValueError as exc:
        raise InvalidParameterError("sdmx", f"unknown agency {agency!r}") from exc


def _dims_from_schema(columns: Sequence[str]) -> tuple[str, ...]:
    """Dimension ids in DSD order, read off the ``{dim}_code`` column sequence.

    The catalog builder emits one ``_code``/``_label`` column pair per DSD
    dimension, in DSD key order — the parquet schema is the declaration.
    """
    return tuple(c[: -len("_code")] for c in columns if c.endswith("_code"))


def _equality_members(predicate: Filter) -> dict[str, list[str]] | None:
    """If *predicate* is a FieldIn / AllOf(FieldIn...) tree, return ``{col: values}``."""
    if isinstance(predicate, FieldIn):
        return {predicate.field: list(predicate.values)}
    if isinstance(predicate, AllOf) and all(isinstance(item, FieldIn) for item in predicate.filters):
        return {item.field: list(item.values) for item in predicate.filters}  # type: ignore[attr-defined]
    return None


def _arrow_filter(predicate: Filter | None) -> ds.Expression | None:
    """Compile a filter tree to a parquet predicate."""
    if predicate is None:
        return None
    return predicate.to_arrow(tuple(predicate.fields()))


def _validate_filter_columns(predicate: Filter, dsd_order: tuple[str, ...], *, label: str) -> None:
    """Reject filter keys that are not real catalog columns.

    A bare dimension id (e.g. ``CURRENCY``) is the common mistake; the column is
    actually ``CURRENCY_code`` / ``CURRENCY_label``. Catch it here with a precise
    hint instead of letting it surface as an opaque pyarrow ``ArrowInvalid``.
    """
    valid = known_search_fields(dsd_order) | {"key"}
    for col in predicate.fields():
        if col in valid:
            continue
        if dim_code_field(col) in valid:
            hint = f"; did you mean {dim_code_field(col)!r}?"
        elif dim_label_field(col) in valid:
            hint = f"; did you mean {dim_label_field(col)!r}?"
        else:
            hint = f"; valid columns: {sorted(valid)}"
        raise InvalidParameterError("sdmx", f"unknown filter column {col!r} for {label}{hint}")


def _dimension_search_hint(col: str, *, agency: str, flow: str) -> str:
    dim_kind = parse_dim_from_field(col)
    if dim_kind is None:
        return ""
    return (
        f"; list populated values with sdmx_dimension_search(agency={agency!r}, "
        f"dataset_id={flow!r}, dimension={dim_kind[0]!r})"
    )


def _filter_autopsy(predicate: Filter, dataset: ds.Dataset, *, agency: str, flow: str) -> str:
    """Per-column breakdown of an empty AND-filter match (error path only).

    For equality-only FieldIn trees, standalone counts rule out typo'd codes and a
    leave-one-out pass names conflicting subsets. Rich pattern filters get a short
    note instead — pattern ops do not decompose cleanly into per-column ``isin``.
    """
    equality = _equality_members(predicate)
    if equality is None:
        return (
            "The filter matched no series in this flow. Relax prefix/contains/matches "
            "constraints, or pin exact codes with equality filter={'{dim}_code': '...'} "
            f"after sdmx_dimension_search(agency={agency!r}, dataset_id={flow!r}, ...)."
        )
    col_exprs = {col: ds.field(col).isin(list(vals)) for col, vals in equality.items() if vals}
    counts = {col: dataset.count_rows(filter=expr) for col, expr in col_exprs.items()}
    lines = [f"  {col}={equality[col]} -> {n} series alone" for col, n in counts.items()]
    zero = [col for col, n in counts.items() if n == 0]
    if zero:
        advice = "Zero-match column(s): " + "; ".join(
            f"{col}" + _dimension_search_hint(col, agency=agency, flow=flow) for col in zero
        )
    elif len(col_exprs) < 2:
        advice = "The filter matches alone but the combined lookup is empty — relax it or re-check the flow."
    else:
        unblocks: list[str] = []
        for col in col_exprs:
            rest = [expr for other, expr in col_exprs.items() if other != col]
            combined = rest[0]
            for item in rest[1:]:
                combined = combined & item
            n = dataset.count_rows(filter=combined)
            if n > 0:
                unblocks.append(f"{col} (-> {n} series)")
        if unblocks:
            advice = (
                "Every column matches >0 series alone. Dropping a single column unblocks the rest: "
                + ", ".join(unblocks)
                + " — the conflict lies among these; relax or re-pick one of them."
            )
        else:
            advice = (
                "Every column matches >0 series alone and no single column unblocks the rest — "
                "the conflict involves 3+ dimensions; relax two or more at a time."
            )
    return "Standalone matches per column:\n" + "\n".join(lines) + f"\n{advice}"


def _empty_match_message(
    query: str | None,
    predicate: Filter | None,
    dataset: ds.Dataset,
    filter_rows: int,
    *,
    agency: str,
    flow: str,
) -> str:
    """Explain an empty match instead of echoing the filter back verbatim.

    Only runs on the error path. Attributes the emptiness to the free-text query
    (the filter alone matched rows) or hands off to :func:`_filter_autopsy` for the
    per-column breakdown.
    """
    label = f"{agency}/{flow}"
    if predicate is None:
        return (
            f"No series matched {query!r} in {label} ({dataset.count_rows()} series in the "
            "flow's catalog). query= matches dimension labels only, never SDMX codes — browse a "
            "dimension's values with sdmx_dimension_search, then pin exact codes with filter=."
        )
    display = _equality_members(predicate) or predicate
    if query is not None and filter_rows > 0:
        return (
            f"No series matched query {query!r} with filter {display} in {label}: "
            f"the filter alone matches {filter_rows} series; the free-text query eliminated "
            "all of them. Relax or drop query=."
        )
    return f"No series matched filter {display} in {label}. " + _filter_autopsy(
        predicate, dataset, agency=agency, flow=flow
    )


def _has_index(catalog: Catalog, field: str) -> bool:
    try:
        catalog.index_for(field)
    except KeyError:
        return False
    return True


#: Every declared dimension-label field weighs the same. A DSD gives no ordering of
#: its dimensions by relevance — REF_AREA is not inherently more identifying than
#: FREQ — and no measurement here supports a skew, so the connector declares parity
#: and lets the caller pin what they actually know with ``filter=``.
DIMENSION_LABEL_WEIGHT = 1.0


def _ranking_fields(catalog: Catalog, dsd_order: tuple[str, ...]) -> dict[str, float]:
    """This connector's ranking policy: equal weight on every indexed dimension label.

    The composed ``title`` stays OFF the surface: it concatenates the very labels
    the label indexes already carry, so scoring it only re-counts matched terms
    (term repetition) — it remains the display column. Code fields stay out too:
    codes are exact identifiers for ``filter=``, and short codes ("A", "M")
    collide with ordinary text.
    """
    return {
        dim_label_field(dim): DIMENSION_LABEL_WEIGHT for dim in dsd_order if _has_index(catalog, dim_label_field(dim))
    }


SERIES_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY),
        Column(name=TITLE_FIELD, role=ColumnRole.TITLE),
        Column(name="*", role=ColumnRole.METADATA),
        *RANKING_COLUMNS,
    ]
)


class SeriesSearchParams(BaseModel):
    agency: Annotated[str, Field(min_length=1, max_length=32)]
    dataset_id: Annotated[str, Field(min_length=1, max_length=128)]
    # Optional: omit for a pure ``filter`` (exact code) enumeration. ``query`` is
    # literal text matched against dimension labels, never against SDMX codes.
    query: str | None = Field(default=None, max_length=512)
    limit: int = Field(default=50, ge=1, le=ENUMERATION_LIMIT)
    # Per-field cap on scored candidate values (the fuzzy/semantic evidence
    # pool), not a result count. Maps to core's candidate_values (default 50).
    top_k_per_dim: int = Field(default=50, ge=1, le=50)
    catalog_root: str | None = None
    filter: Any = Field(default=None)


_SERIES_SEARCH_DOC = """Search populated series keys in a prebuilt catalog for one SDMX flow.

``query=`` ranks free text against dimension labels (exploratory shortlist). ``filter=``
commits: equality shorthand (``{"FREQ_code": "M"}``), ``F(...)`` (``.eq`` / ``.is_in`` /
``.prefix`` / ``.contains`` / ``.matches``), or ``{"field": "key", "prefix": "..."}``.
Paste returned ``key`` into ``sdmx_fetch`` as ``series_ref``.

For contested dimensions, resolve codes via ``sdmx_dimension_search`` then re-search with
``filter=`` pinning ``{dim}_code``. Ranked shortlist ``limit`` <= 500; omit ``query=`` to
enumerate a filter slice (<= 10000). Columns: ``key``, ``{dim}_code``/``{dim}_label``, score, search_detail.
"""


@connector(output=SERIES_SEARCH_OUTPUT, tags=["sdmx", "tool"], description=_SERIES_SEARCH_DOC)
def sdmx_series_search(
    agency: str,
    dataset_id: str,
    query: str | None = None,
    limit: int = 50,
    top_k_per_dim: int = 50,
    catalog_root: str | None = None,
    filter: FilterLike | None = None,
) -> pd.DataFrame:
    params = SeriesSearchParams(
        agency=agency,
        dataset_id=dataset_id,
        query=query,
        limit=limit,
        top_k_per_dim=top_k_per_dim,
        catalog_root=catalog_root,
        filter=filter,
    )
    agency_id = _parse_agency(params.agency)
    flow = params.dataset_id.strip()
    q = (params.query or "").strip() or None
    predicate = as_filter(params.filter)
    if q is None and predicate is None:
        raise InvalidParameterError(
            "sdmx",
            "provide query= (literal text over dimension labels) and/or filter= "
            "({dim}_code / key constraints via equality or F(...)/expression)",
        )
    if q is not None and params.limit > RANKED_LIMIT:
        raise InvalidParameterError(
            "sdmx",
            f"query= is a ranked shortlist (limit <= {RANKED_LIMIT}). To read a whole "
            "dimension slice, omit query= and enumerate the cached catalog with "
            f"filter= (limit up to {ENUMERATION_LIMIT}).",
        )

    namespace = series_namespace(agency_id, flow)
    label = f"{agency_id.value}/{flow}"
    catalog_path = _resolve_catalog_path(namespace, label=label, catalog_root=params.catalog_root)
    if not catalog_path.is_dir():
        raise ConnectorError(_not_published(label), provider="sdmx")

    try:
        catalog = _load_series_catalog(namespace, str(catalog_path.resolve()))
        meta = read_meta(catalog_path)
        if meta.backend.kind != "parquet":
            raise ConnectorError(f"Series catalog at {catalog_path} is not parquet-backed", provider="sdmx")
    except (FileNotFoundError, ValueError) as exc:
        raise ConnectorError(f"Invalid series catalog for {namespace}: {exc}", provider="sdmx") from exc

    parquet_path = catalog_path / (meta.backend.rows_filename or SERIES_PARQUET)
    dataset = ds.dataset(str(parquet_path), format="parquet")
    dsd_order = _dims_from_schema(dataset.schema.names)

    if predicate is not None:
        _validate_filter_columns(predicate, dsd_order, label=f"{agency_id.value}/{flow}")

    if q is not None:
        # One weighted pass over the candidate rows. Each match already carries its
        # title and every dimension column, so the row schema below needs no second
        # parquet scan to look anything up.
        ranking_fields = _ranking_fields(catalog, dsd_order)
        matches = catalog.multi_field_search(
            q,
            fields=ranking_fields,
            filter=predicate,
            limit=params.limit,
            candidate_values=params.top_k_per_dim,
        )
        rows = [
            _row(match.code, match.title, match.metadata, dsd_order, match.score, match.search_detail)
            for match in matches
        ]
    else:
        rows = [
            _row(str(row.get("key", "")), str(row.get(TITLE_FIELD) or ""), row, dsd_order, None, None)
            for row in islice(catalog.iter_rows(filter=predicate), params.limit)
        ]

    if not rows:
        arrow = _arrow_filter(predicate)
        filter_rows = dataset.count_rows(filter=arrow) if arrow is not None else 0
        raise EmptyDataError(
            "sdmx",
            _empty_match_message(
                q,
                predicate,
                dataset,
                filter_rows,
                agency=agency_id.value,
                flow=flow,
            ),
        )

    return pd.DataFrame(rows)


def _row(
    code: str,
    title: str,
    columns: Mapping[str, Any],
    dsd_order: tuple[str, ...],
    score: float | None,
    search_detail: SearchDetail | None,
) -> dict[str, object]:
    """One output row, from either the ranked or the enumerated read.

    ``key`` is the catalog column value unchanged — the same string ``filter=`` matches.
    """
    row: dict[str, object] = {
        "key": code,
        TITLE_FIELD: title,
        "score": wire_score(score),
        "search_detail": wire_search_detail(search_detail),
    }
    for dim in dsd_order:
        for column in (dim_code_field(dim), dim_label_field(dim)):
            row[column] = columns.get(column, "")
    return row


__all__ = [
    "_clear_series_catalog_lru",
    "_not_published",
    "sdmx_series_search",
]
