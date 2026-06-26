"""``sdmx_series_search`` — columnar per-flow series discovery from local catalogs."""

from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import pyarrow.dataset as ds
from parsimony.catalog import Catalog, resolve_catalog_dir
from parsimony.catalog.search import resolved_catalog_url
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.catalog.storage import read_meta
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_sdmx.catalog_series import series_namespace
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.search import DEFAULT_CATALOG_ROOT, PARSIMONY_SDMX_CATALOG_URL_ENV
from parsimony_sdmx.series_facets import facets_from_table, facets_to_json
from parsimony_sdmx.series_fields import (
    SERIES_PARQUET,
    TITLE_FIELD,
    dim_code_field,
    dim_label_field,
    known_search_fields,
)
from parsimony_sdmx.series_query import plan_series_search

logger = logging.getLogger(__name__)

DEFAULT_LRU_SIZE = 4


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


def _resolve_catalog_path(namespace: str, *, catalog_root: str | None = None) -> Path:
    """Resolve this flow's catalog to a local directory (for parquet + Catalog.load).

    Delegates URL resolution to the framework: ``resolve_catalog_dir`` handles
    every scheme (``file://`` and ``hf://``) and, for a sub-path ``hf://`` catalog,
    downloads only this flow's sub-tree rather than enumerating the whole SDMX
    monorepo. The connector holds no scheme knowledge of its own.
    """
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


def _parse_agency(agency: str) -> AgencyId:
    raw = agency.strip().upper()
    if not raw:
        raise InvalidParameterError("sdmx", "agency must be non-empty")
    try:
        return AgencyId(raw)
    except ValueError as exc:
        raise InvalidParameterError("sdmx", f"unknown agency {agency!r}") from exc


def _sdmx_meta(catalog_dir: Path) -> dict[str, Any]:
    raw = json.loads((catalog_dir / "meta.json").read_text(encoding="utf-8"))
    sdmx = raw.get("sdmx")
    if isinstance(sdmx, dict):
        return sdmx
    return {}


def _dim_columns(dsd_order: tuple[str, ...]) -> list[str]:
    cols: list[str] = []
    for dim in dsd_order:
        cols.append(dim_code_field(dim))
        cols.append(dim_label_field(dim))
    return cols


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


SERIES_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY),
        Column(name=TITLE_FIELD, role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
        Column(name="refine", role=ColumnRole.METADATA),
    ]
)


class SeriesSearchParams(BaseModel):
    agency: Annotated[str, Field(min_length=1, max_length=32)]
    dataset_id: Annotated[str, Field(min_length=1, max_length=128)]
    # Optional: omit for a pure ``filter_json`` (exact code) lookup. Free-text
    # ``query`` is matched against titles/labels, never against SDMX codes.
    query: str | None = Field(default=None, max_length=512)
    limit: int = Field(default=50, ge=1, le=500)
    top_k_per_dim: int = Field(default=5, ge=1, le=50)
    catalog_root: str | None = None
    field: str | None = Field(default=None, max_length=128)
    filter_json: str | None = Field(default=None, max_length=4096)


@connector(output=SERIES_SEARCH_OUTPUT, tags=["sdmx", "tool"])
def sdmx_series_search(
    agency: str,
    dataset_id: str,
    query: str | None = None,
    limit: int = 50,
    top_k_per_dim: int = 5,
    catalog_root: str | None = None,
    field: str | None = None,
    filter_json: str | None = None,
) -> pd.DataFrame:
    """Search populated series keys in a prebuilt columnar catalog for one SDMX flow.

    ``query=`` is FREE TEXT matched against human-readable series titles/labels
    (e.g. "10-year government bond spot rate") — do NOT pass SDMX codes
    ("SR_10Y", "10Y"); they are absent from titles and match nothing. To match
    exact codes, target the ``{dim}_code``/``{dim}_label`` columns via
    ``filter_json`` (exact AND filter, e.g. ``{"DATA_TYPE_FM_code": ["SR_10Y"]}``)
    or ``field=`` with a single ``query=`` scoped to one such column; ``query=``
    may be omitted for a pure ``filter_json`` lookup. Returns ranked matches with
    the series ``key``, ``title``, the resolved ``{dim}_code``/``{dim}_label``
    columns, and a ``refine`` facet column.
    """
    params = SeriesSearchParams(
        agency=agency,
        dataset_id=dataset_id,
        query=query,
        limit=limit,
        top_k_per_dim=top_k_per_dim,
        catalog_root=catalog_root,
        field=field,
        filter_json=filter_json,
    )
    agency_id = _parse_agency(params.agency)
    flow = params.dataset_id.strip()
    q = (params.query or "").strip() or None
    if q is None and params.filter_json is None:
        raise InvalidParameterError(
            "sdmx",
            "provide query= (free-text over titles/labels) and/or filter_json= (exact {dim}_code filters)",
        )
    if params.field is not None and q is None:
        raise InvalidParameterError("sdmx", "field= requires a non-empty query=")

    namespace = series_namespace(agency_id, flow)
    catalog_path = _resolve_catalog_path(namespace, catalog_root=params.catalog_root)
    if not catalog_path.is_dir():
        raise ConnectorError(
            f"No series catalog for {agency_id.value}/{flow} at {catalog_path}",
            provider="sdmx",
        )

    try:
        catalog = _load_series_catalog(namespace, str(catalog_path.resolve()))
        meta = read_meta(catalog_path)
        if meta.backend.kind != "parquet":
            raise ConnectorError(f"Series catalog at {catalog_path} is not parquet-backed", provider="sdmx")
    except (FileNotFoundError, ValueError) as exc:
        raise ConnectorError(f"Invalid series catalog for {namespace}: {exc}", provider="sdmx") from exc

    sdmx_meta = _sdmx_meta(catalog_path)
    dsd_order = tuple(sdmx_meta.get("dsd_order") or ())
    if params.field is not None or params.filter_json is not None:
        filter_spec: dict[str, list[str]] = {}
        if params.filter_json:
            try:
                parsed_filter = json.loads(params.filter_json)
            except json.JSONDecodeError as exc:
                raise InvalidParameterError("sdmx", f"filter_json must be valid JSON: {exc}") from exc
            if not isinstance(parsed_filter, dict):
                raise InvalidParameterError("sdmx", "filter_json must be a JSON object")
            filter_spec = {}
            for key, values in parsed_filter.items():
                if not isinstance(values, list):
                    raise InvalidParameterError("sdmx", f"filter_json[{key!r}] must be a list of values")
                filter_spec[str(key)] = [str(v) for v in values]
            _validate_filter_columns(filter_spec, dsd_order, label=f"{agency_id.value}/{flow}")
        plan_query = q if params.field is not None else None
        plan_field = params.field
        pinned_dims: frozenset[str] = frozenset()
    else:
        # No field and no filter_json: the guard above guarantees q is set here.
        assert q is not None
        plan = plan_series_search(
            q,
            catalog=catalog,
            dsd_order=dsd_order,
            top_k_per_dim=params.top_k_per_dim,
        )
        plan_query = plan.query
        plan_field = plan.field
        filter_spec = plan.filter
        pinned_dims = plan.pinned_dims

    matches = catalog.search(
        plan_query,
        limit=params.limit,
        field=plan_field,
        filter=filter_spec or None,
        top_k_values=params.top_k_per_dim,
    )

    parquet_path = catalog_path / (meta.backend.rows_filename or SERIES_PARQUET)
    dataset = ds.dataset(str(parquet_path), format="parquet")
    expr = None
    if filter_spec:
        import pyarrow.dataset as pds

        exprs = [pds.field(col).isin(vals) for col, vals in filter_spec.items() if vals]
        if exprs:
            expr = exprs[0]
            for item in exprs[1:]:
                expr = expr & item
    filtered = dataset.to_table(filter=expr, columns=["key", "title", *_dim_columns(dsd_order)])
    facets = facets_from_table(filtered, dsd_order, pinned_dims=set(pinned_dims))
    refine_json = facets_to_json(facets)

    if not matches:
        criteria = repr(q) if q is not None else f"filter {filter_spec}"
        raise EmptyDataError("sdmx", f"No series matched {criteria} in {agency_id.value}/{flow}")

    # ``filtered`` already holds key+title (materialized for facets); reuse it to
    # surface the human-readable title without a second parquet scan.
    matched_codes = {match.code for match in matches}
    title_map = {
        key: title
        for key, title in zip(filtered.column("key").to_pylist(), filtered.column(TITLE_FIELD).to_pylist(), strict=True)
        if key in matched_codes
    }

    rows: list[dict[str, object]] = []
    for match in matches:
        row: dict[str, object] = {
            "key": match.code,
            TITLE_FIELD: title_map.get(match.code, ""),
            "score": round(match.score, 6),
            "refine": refine_json,
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
    "sdmx_series_search",
]
