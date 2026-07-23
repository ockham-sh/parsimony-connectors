"""``sdmx_series_search`` — columnar per-flow series discovery from local catalogs."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Sequence
from functools import lru_cache
from pathlib import Path
from typing import Annotated

import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
from parsimony.catalog import Catalog, resolve_catalog_dir
from parsimony.catalog.search import RANKING_COLUMNS, resolved_catalog_url
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.catalog.storage import read_meta
from parsimony.connector import connector
from parsimony.errors import CatalogNotFoundError, ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputSpec
from pydantic import BaseModel, Field

from parsimony_sdmx.catalog_series import _strip_flow_prefix
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
    searchable_fields,
    title_not_searchable_error,
)
from parsimony_sdmx.series_query import plan_series_search

logger = logging.getLogger(__name__)

DEFAULT_LRU_SIZE = 4

#: A free-text ``query`` is a ranked shortlist for reading — capped small. A pure
#: ``filter_json`` lookup is an *enumeration* of the already-cached local catalog into a
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


def _parse_filter_json(filter_json: str) -> dict[str, list[str]]:
    """Parse a ``filter_json`` string into ``{column: [values]}``.

    Accepts a bare scalar as a single-code filter: ``{"FREQ_code": "M"}`` means ``["M"]``.
    A str is iterable, so it must be wrapped, never iterated — otherwise "DE" would expand
    to ``["D", "E"]`` and match nothing. Shared by ``sdmx_series_search`` and
    ``sdmx_dimension_search`` so both accept the exact same filter syntax.
    """
    try:
        parsed = json.loads(filter_json)
    except json.JSONDecodeError as exc:
        raise InvalidParameterError("sdmx", f"filter_json must be valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise InvalidParameterError("sdmx", "filter_json must be a JSON object")
    return {
        str(key): [str(v) for v in (values if isinstance(values, list) else [values])] for key, values in parsed.items()
    }


def _validate_filter_columns(filter_spec: dict[str, list[str]], dsd_order: tuple[str, ...], *, label: str) -> None:
    """Reject filter keys that are not real catalog columns.

    A bare dimension id (e.g. ``CURRENCY``) is the common mistake; the column is
    actually ``CURRENCY_code`` / ``CURRENCY_label``. Catch it here with a precise
    hint instead of letting it surface as an opaque pyarrow ``ArrowInvalid``.
    """
    valid = known_search_fields(dsd_order) | {"key"}
    for col in filter_spec:
        if col in valid:
            continue
        if dim_code_field(col) in valid:
            hint = f"; did you mean {dim_code_field(col)!r}?"
        elif dim_label_field(col) in valid:
            hint = f"; did you mean {dim_label_field(col)!r}?"
        else:
            hint = f"; valid columns: {sorted(valid)}"
        raise InvalidParameterError("sdmx", f"unknown filter column {col!r} for {label}{hint}")


def _validate_search_fields(fields: str | list[str], dsd_order: tuple[str, ...]) -> None:
    """Reject a ``fields=`` scope that names something the catalog cannot score.

    ``title`` is the one that catches people out: it is a real column and a real
    output, so asking to search it looks reasonable — but it is composed from the
    dimension labels at build time and carries no index. Say so, rather than
    silently returning nothing.
    """
    requested = [fields] if isinstance(fields, str) else list(fields)
    valid = searchable_fields(dsd_order)
    for name in requested:
        if name in valid:
            continue
        if name == TITLE_FIELD:
            raise title_not_searchable_error()
        raise InvalidParameterError("sdmx", f"unknown search field {name!r}; valid fields: {sorted(valid)}")


def _dimension_search_hint(col: str, *, agency: str, flow: str) -> str:
    dim_kind = parse_dim_from_field(col)
    if dim_kind is None:
        return ""
    return (
        f"; list populated values with sdmx_dimension_search(agency={agency!r}, "
        f"dataset_id={flow!r}, dimension={dim_kind[0]!r})"
    )


def _validate_filter_values(
    filter_spec: dict[str, list[str]],
    dataset: ds.Dataset,
    *,
    agency: str,
    flow: str,
) -> None:
    """Reject filter values that are not populated anywhere in the flow.

    ``isin`` semantics silently drop a value the flow never populates ("EL" where ECB
    uses "GR": 11 requested, 10 returned, no signal). Mirror ``_validate_filter_columns``
    one level down: a filter that references anything the flow doesn't have — column or
    value — is an invalid parameter, caught eagerly with the culprit named.
    """
    cols = [col for col, vals in filter_spec.items() if vals]
    if not cols:
        return
    table = dataset.to_table(columns=cols)
    problems: list[str] = []
    for col in cols:
        requested = filter_spec[col]
        populated = set(pc.unique(table.column(col)).to_pylist())  # type: ignore[attr-defined]
        missing = [v for v in requested if v not in populated]
        if not missing:
            continue
        kept = len(requested) - len(missing)
        problems.append(
            f"{col} value(s) {missing} not populated ({kept} of {len(requested)} requested values exist)"
            + _dimension_search_hint(col, agency=agency, flow=flow)
        )
    if problems:
        raise InvalidParameterError("sdmx", f"filter values not found in {agency}/{flow}: " + "; ".join(problems))


def _filter_autopsy(filter_spec: dict[str, list[str]], dataset: ds.Dataset, *, agency: str, flow: str) -> str:
    """Per-column breakdown of an empty AND-filter match (error path only).

    Standalone counts rule out typo'd codes. When every column matches alone, a
    leave-one-out pass (count of all the OTHER columns ANDed) names the conflicting
    subset: a column whose removal unblocks the rest is part of the conflict — for a
    pairwise conflict, exactly the two conflicting columns light up. O(2n) counted
    scans of the local parquet, paid only when the match is already empty.
    """
    col_exprs = {col: ds.field(col).isin(vals) for col, vals in filter_spec.items() if vals}
    counts = {col: dataset.count_rows(filter=expr) for col, expr in col_exprs.items()}
    lines = [f"  {col}={filter_spec[col]} -> {n} series alone" for col, n in counts.items()]
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
    plan_query: str | None,
    filter_spec: dict[str, list[str]],
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
    if not filter_spec:
        return (
            f"No series matched {plan_query!r} in {label} ({dataset.count_rows()} series in the "
            "flow's catalog). query= matches titles/labels only, never SDMX codes — browse a "
            "dimension's values with sdmx_dimension_search, or filter exact codes with filter_json."
        )
    if plan_query is not None and filter_rows > 0:
        return (
            f"No series matched query {plan_query!r} with filter {filter_spec} in {label}: "
            f"the filter alone matches {filter_rows} series; the free-text query eliminated "
            "all of them. Relax or drop query=."
        )
    return f"No series matched filter {filter_spec} in {label}. " + _filter_autopsy(
        filter_spec, dataset, agency=agency, flow=flow
    )


def _has_index(catalog: Catalog, field: str) -> bool:
    try:
        catalog.index_for(field)
    except KeyError:
        return False
    return True


def _search_surface(
    catalog: Catalog,
    plan_query: str | None,
    plan_fields: str | list[str] | None,
    dsd_order: tuple[str, ...],
) -> str | list[str] | None:
    """Declare the scoring surface for one catalog search.

    A scoped query keeps its caller-declared field(s). A bare query spans
    every indexed dimension-label field, so a query naming dimension values
    ("current account … quarterly") earns coverage on those slices. The
    composed ``title`` stays OFF the surface: it concatenates the very labels
    the label indexes already carry, so scoring it only re-counts matched
    terms (term repetition) — it remains the display column. Code fields stay
    out: codes are exact identifiers for filter_json, and short codes
    ("A", "M") collide with ordinary text.
    """
    if plan_fields is not None:
        return plan_fields
    if plan_query is None:
        return None
    surface = [dim_label_field(dim) for dim in dsd_order]
    return [name for name in surface if _has_index(catalog, name)] or None


SERIES_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY),
        Column(name=TITLE_FIELD, role=ColumnRole.TITLE),
        *RANKING_COLUMNS,
    ]
)


class SeriesSearchParams(BaseModel):
    agency: Annotated[str, Field(min_length=1, max_length=32)]
    dataset_id: Annotated[str, Field(min_length=1, max_length=128)]
    # Optional: omit for a pure ``filter_json`` (exact code) lookup. Free-text
    # ``query`` is matched against titles/labels, never against SDMX codes.
    query: str | None = Field(default=None, max_length=512)
    limit: int = Field(default=50, ge=1, le=ENUMERATION_LIMIT)
    # Per-field cap on scored candidate values (the fuzzy/semantic evidence
    # pool), not a result count. Maps to core's top_k_values (default 50).
    top_k_per_dim: int = Field(default=50, ge=1, le=50)
    catalog_root: str | None = None
    # One field name scopes the query to that surface; a list fuses several.
    fields: str | list[str] | None = Field(default=None)
    filter_json: str | None = Field(default=None, max_length=4096)


@connector(output=SERIES_SEARCH_OUTPUT, tags=["sdmx", "tool"])
def sdmx_series_search(
    agency: str,
    dataset_id: str,
    query: str | None = None,
    limit: int = 50,
    top_k_per_dim: int = 50,
    catalog_root: str | None = None,
    fields: str | list[str] | None = None,
    filter_json: str | None = None,
) -> pd.DataFrame:
    """Search populated series keys in a prebuilt columnar catalog for one SDMX flow.

    ``query=`` is FREE TEXT over dimension labels — NOT SDMX codes; filter exact codes with
    ``filter_json`` (AND on ``{dim}_code``/``{dim}_label``). Columns add ``key`` (→ sdmx_fetch)
    and ``{dim}_code``/``{dim}_label``. ``fields=`` scopes the query; ``top_k_per_dim`` caps
    candidates per field, not rows. Ranked shortlist (``limit`` <= 500); omit ``query=`` to
    enumerate via ``filter_json`` (<= 10000).
    """
    params = SeriesSearchParams(
        agency=agency,
        dataset_id=dataset_id,
        query=query,
        limit=limit,
        top_k_per_dim=top_k_per_dim,
        catalog_root=catalog_root,
        fields=fields,
        filter_json=filter_json,
    )
    agency_id = _parse_agency(params.agency)
    flow = params.dataset_id.strip()
    q = (params.query or "").strip() or None
    if q is None and params.filter_json is None:
        raise InvalidParameterError(
            "sdmx",
            "provide query= (free-text over dimension labels) and/or filter_json= (exact {dim}_code filters)",
        )
    if params.fields is not None and q is None:
        raise InvalidParameterError("sdmx", "fields= requires a non-empty query=")
    if q is not None and params.limit > RANKED_LIMIT:
        raise InvalidParameterError(
            "sdmx",
            f"query= is a ranked shortlist (limit <= {RANKED_LIMIT}). To read a whole "
            "dimension slice, omit query= and enumerate the cached catalog with "
            f"filter_json= (limit up to {ENUMERATION_LIMIT}).",
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
    if params.fields is not None:
        _validate_search_fields(params.fields, dsd_order)
    if params.fields is not None or params.filter_json is not None:
        filter_spec: dict[str, list[str]] = {}
        if params.filter_json:
            filter_spec = _parse_filter_json(params.filter_json)
            _validate_filter_columns(filter_spec, dsd_order, label=f"{agency_id.value}/{flow}")
            _validate_filter_values(filter_spec, dataset, agency=agency_id.value, flow=flow)
        # Honor query= alongside filter_json=: rank the filtered slice by the
        # query (bare-query surface when no fields= is declared) instead of
        # dropping it and returning the slice unranked.
        plan_query = q
        plan_fields: str | list[str] | None = params.fields
    else:
        # No fields and no filter_json: the guard above guarantees q is set here.
        assert q is not None
        plan = plan_series_search(
            q,
            catalog=catalog,
            dsd_order=dsd_order,
            top_k_per_dim=params.top_k_per_dim,
        )
        plan_query = plan.query
        plan_fields = plan.field
        filter_spec = plan.filter

    matches = catalog.search(
        plan_query,
        limit=params.limit,
        fields=_search_surface(catalog, plan_query, plan_fields, dsd_order),
        filter=filter_spec or None,
        top_k_values=params.top_k_per_dim,
    )

    expr = None
    for col, vals in filter_spec.items():
        if not vals:
            continue
        item = ds.field(col).isin(vals)
        expr = item if expr is None else expr & item
    filtered = dataset.to_table(filter=expr, columns=["key", "title"])

    if not matches:
        raise EmptyDataError(
            "sdmx",
            _empty_match_message(
                plan_query,
                filter_spec,
                dataset,
                filtered.num_rows,
                agency=agency_id.value,
                flow=flow,
            ),
        )

    # ``filtered`` holds key+title; reuse it to surface the human-readable title
    # without a second parquet scan.
    matched_codes = {match.code for match in matches}
    title_map = {
        key: title
        for key, title in zip(filtered.column("key").to_pylist(), filtered.column(TITLE_FIELD).to_pylist(), strict=True)
        if key in matched_codes
    }

    rows: list[dict[str, object]] = []
    for match in matches:
        # Old published catalogs can carry the flow id as a key prefix ("YC.B.U2...");
        # new builds strip it at build time. Strip at read time too so the emitted key
        # always equals sdmx_fetch's bare series_key (title_map lookups stay raw).
        row: dict[str, object] = {
            "key": _strip_flow_prefix(match.code, flow),
            TITLE_FIELD: title_map.get(match.code, ""),
            "coverage": round(match.coverage, 6),
            "score": round(match.score, 6),
            "matched": match.matched,
        }
        for dim in dsd_order:
            code_col = dim_code_field(dim)
            label_col = dim_label_field(dim)
            row[code_col] = match.metadata.get(code_col, "")
            row[label_col] = match.metadata.get(label_col, "")
        rows.append(row)

    return pd.DataFrame(rows)


__all__ = [
    "_clear_series_catalog_lru",
    "_not_published",
    "sdmx_series_search",
]
