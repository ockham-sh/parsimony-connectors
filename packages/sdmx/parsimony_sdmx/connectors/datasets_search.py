"""``sdmx_datasets_search`` — discover SDMX flows across per-agency dataset catalogs.

**Usual path:**

1. ``sdmx_datasets_search`` — find the flow; its ``dimensions`` are the axes it breaks down by.
2. ``sdmx_series_search`` — find and filter that flow's series;
   ``sdmx_dimension_search`` — resolve a dimension's valid codes.
3. ``sdmx_fetch`` — pull observations for the chosen key(s).

Only published flows are searchable: a flow with no series catalog hard-errors (ask the
maintainers to build it). There is no live fallback.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import pandas as pd
from parsimony.catalog import Catalog, CatalogMatch
from parsimony.catalog.search import RANKING_COLUMNS, CatalogLRU, resolved_catalog_url
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputSpec
from pydantic import BaseModel, Field

from parsimony_sdmx.catalog_build import build_agency_datasets_catalog
from parsimony_sdmx.core.agencies import ALL_AGENCIES, AgencyId
from parsimony_sdmx.core.namespaces import datasets_namespace

logger = logging.getLogger(__name__)

PARSIMONY_SDMX_CATALOG_URL_ENV = "PARSIMONY_SDMX_CATALOG_URL"
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/sdmx"
DEFAULT_LRU_SIZE = 8


def _lru_size_from_env() -> int:
    raw = os.environ.get("PARSIMONY_SDMX_CATALOG_LRU_SIZE", "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


_lru = CatalogLRU(_lru_size_from_env())


def _get_or_load_catalog(
    namespace: str,
    *,
    catalog_root: str | None = None,
    build=None,
):
    root = resolved_catalog_url(
        PARSIMONY_SDMX_CATALOG_URL_ENV,
        DEFAULT_CATALOG_ROOT,
        override=catalog_root,
    )
    url = f"{root}/{namespace}"
    cache_path = lazy_catalog_dir("sdmx", namespace)
    return _lru.get_or_load(url, cache_path=cache_path, build=build)


def _clear_catalog_lru() -> None:
    _lru.clear()


def set_catalog_lru_size(size: int) -> None:
    global _lru
    if size < 1:
        raise InvalidParameterError("sdmx", "catalog_lru_size must be >= 1")
    _lru = CatalogLRU(size)


def _parse_agency(agency: str) -> AgencyId:
    raw = agency.strip().upper()
    if not raw:
        raise ConnectorError("agency must be non-empty (e.g. 'ECB').", provider="sdmx")
    try:
        return AgencyId(raw)
    except ValueError:
        raise ConnectorError(
            f"Unknown agency {agency!r}. Supported: {[a.value for a in AgencyId]}.",
            provider="sdmx",
        ) from None


def _agencies_for_search(agency: str | None) -> list[AgencyId]:
    if agency is None or not agency.strip():
        return list(ALL_AGENCIES)
    return [_parse_agency(agency)]


DATASETS_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="dataset_ref", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(name="dimensions", role=ColumnRole.METADATA),
        *RANKING_COLUMNS,
    ]
)


class DatasetsSearchParams(BaseModel):
    query: Annotated[str, Field(min_length=1, max_length=512)]
    agency: Annotated[str | None, Field(default=None, max_length=32, description="SDMX agency (optional).")]
    limit: int = Field(default=10, ge=1, le=50)
    catalog_root: str | None = None


@connector(output=DATASETS_SEARCH_OUTPUT, tags=["sdmx", "tool"])
def sdmx_datasets_search(
    query: str,
    agency: str | None = None,
    limit: int = 10,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Discover SDMX flows within one or all agency dataset catalogs.

    Scope with ``agency=`` when known; unscoped merges every agency, risking a miss.
    Rows rank facts first: an exact ``title`` hit is ``coverage`` 1.0 and pins above
    any fuzzy ``score``. An all-'semantic' ``matched`` page matched nothing lexically
    real — rephrase.

    Next: a hit's ``agency``+``dataset_id`` feed ``sdmx_series_search``/
    ``sdmx_dimension_search``; ``dataset_ref`` feeds ``sdmx_fetch``.

    ``dimensions`` are the axes a flow breaks down by, in key order; granularity
    (e.g. ``geo``: 38 vs 1,247 values) only ``sdmx_dimension_search`` shows. Empty
    means uncaptured, not none.

    Relevance-ranked top-N (``limit`` <= 50).
    """
    params = DatasetsSearchParams(query=query, agency=agency, limit=limit, catalog_root=catalog_root)
    agencies = _agencies_for_search(params.agency)

    # Sort key per row: (not exact-pin, per-agency rank, -score). Exact-pin
    # (coverage 1.0) rows lead; the rest interleave by each row's position in
    # its own agency's ranked list (RRF over disjoint per-agency rankings),
    # with score as the tiebreak; coverage below 1.0 does not otherwise order
    # this single-field surface.
    all_matches: list[tuple[tuple[bool, int, float], CatalogMatch]] = []
    for parsed_agency in agencies:
        namespace = datasets_namespace(parsed_agency)

        def _build(agency=parsed_agency) -> Catalog:
            return build_agency_datasets_catalog(agency)

        catalog = _get_or_load_catalog(namespace, catalog_root=params.catalog_root, build=_build)
        # Titles only: a flow's identity is its title. DSD-vocabulary text
        # ranks flows that break down BY a subject above flows ABOUT it.
        matches = catalog.search(params.query, limit=params.limit, fields=["title"])
        for rank, m in enumerate(matches):
            all_matches.append(((m.coverage != 1.0, rank, -m.score), m))

    if not all_matches:
        scope = params.agency or "all agencies"
        raise EmptyDataError(
            provider="sdmx",
            message=(f"No flow matches for query={params.query!r} in {scope!r}. Try a broader title query."),
        )

    all_matches.sort(key=lambda item: item[0])
    top = all_matches[: params.limit]

    rows: list[dict[str, object]] = []
    for _, m in top:
        # The catalog key is '{agency}|{dataset_id}'; sdmx_fetch wants 'AGENCY-DATASET_ID'.
        row_agency, sep, dataset_id = m.code.partition("|")
        rows.append(
            {
                "dataset_ref": f"{row_agency}-{dataset_id}" if sep else m.code,
                "title": m.title,
                "agency": row_agency if sep else "",
                "dataset_id": dataset_id,
                "dimensions": m.metadata.get("dimensions", []) if m.metadata else [],
                "coverage": round(m.coverage, 6),
                "score": round(m.score, 6),
                "matched": m.matched,
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "DEFAULT_CATALOG_ROOT",
    "DatasetsSearchParams",
    "PARSIMONY_SDMX_CATALOG_URL_ENV",
    "_clear_catalog_lru",
    "sdmx_datasets_search",
    "set_catalog_lru_size",
]
