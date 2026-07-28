"""``sdmx_datasets_search`` — discover SDMX flows in one agency's dataset catalog.

**Usual path:**

1. ``sdmx_datasets_search`` — find the flow; its ``dimensions`` are the axes it breaks down by.
2. ``sdmx_series_search`` — one exploratory ``query=`` shortlist, then pin contested
   dimensions with ``filter=``; ``sdmx_dimension_search`` resolves unknown codes.
3. ``sdmx_fetch`` — pull observations for the chosen key(s).

Only published flows are searchable: a flow with no series catalog hard-errors (ask the
maintainers to build it). There is no live fallback.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import pandas as pd
from parsimony.catalog import Catalog
from parsimony.catalog.search import RANKING_COLUMNS, CatalogLRU, resolved_catalog_url, wire_score, wire_search_detail
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputSpec
from pydantic import BaseModel, Field

from parsimony_sdmx.catalog_build import build_agency_datasets_catalog
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.core.namespaces import datasets_namespace

logger = logging.getLogger(__name__)

PARSIMONY_SDMX_CATALOG_URL_ENV = "PARSIMONY_SDMX_CATALOG_URL"
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/sdmx"
DEFAULT_LRU_SIZE = 8

_SUPPORTED_AGENCIES = ", ".join(a.value for a in AgencyId)


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
        raise ConnectorError(
            f"agency must be non-empty. Supported: [{_SUPPORTED_AGENCIES}].",
            provider="sdmx",
        )
    try:
        return AgencyId(raw)
    except ValueError:
        raise ConnectorError(
            f"Unknown agency {agency!r}. Supported: [{_SUPPORTED_AGENCIES}].",
            provider="sdmx",
        ) from None


DATASETS_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="dataset_id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(
            name="dimensions",
            role=ColumnRole.METADATA,
            description="The axes this flow breaks down by, in key order",
        ),
        *RANKING_COLUMNS,
    ]
)


class DatasetsSearchParams(BaseModel):
    query: Annotated[str, Field(min_length=1, max_length=512)]
    agency: Annotated[
        str,
        Field(
            min_length=1,
            max_length=32,
            description=f"SDMX agency (required). One of: {_SUPPORTED_AGENCIES}.",
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50)
    catalog_root: str | None = None


@connector(output=DATASETS_SEARCH_OUTPUT, tags=["sdmx", "tool"])
def sdmx_datasets_search(
    query: str,
    agency: str,
    limit: int = 10,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Discover SDMX flows in one agency's dataset catalog.

    ``agency`` is required — each agency is its own catalog/index, so scores stay
    on one scale. Supported: ``ECB``, ``ESTAT``, ``IMF_DATA``, ``WB_WDI``. If the
    source agency is unknown, call once per agency and compare titles yourself;
    there is no cross-agency merge.

    A hit is a flow candidate. Paste ``agency`` and ``dataset_id`` into
    ``sdmx_series_search`` / ``sdmx_fetch`` (and ``sdmx_dimension_search`` when
    needed). Series identity comes next via ``sdmx_series_search`` (shortlist with
    ``query=``, then pin contested dimensions with ``filter=`` /
    ``sdmx_dimension_search``).

    Relevance-ranked top-N (``limit`` <= 50) on flow titles.
    """
    params = DatasetsSearchParams(query=query, agency=agency, limit=limit, catalog_root=catalog_root)
    parsed_agency = _parse_agency(params.agency)
    namespace = datasets_namespace(parsed_agency)

    def _build() -> Catalog:
        return build_agency_datasets_catalog(parsed_agency)

    catalog = _get_or_load_catalog(namespace, catalog_root=params.catalog_root, build=_build)
    # Titles only: a flow's identity is its title. DSD-vocabulary text
    # ranks flows that break down BY a subject above flows ABOUT it.
    matches = catalog.search(params.query, limit=params.limit, field="title")
    if not matches:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No flow matches for query={params.query!r} in agency={parsed_agency.value!r}. "
                "Try a broader title query, or search another agency "
                f"({_SUPPORTED_AGENCIES})."
            ),
        )

    rows: list[dict[str, object]] = []
    for m in matches:
        # Catalog Entity code is storage-only '{agency}|{dataset_id}'.
        row_agency, sep, dataset_id = m.code.partition("|")
        rows.append(
            {
                "dataset_id": dataset_id if sep else m.code,
                "title": m.title,
                "agency": row_agency if sep else parsed_agency.value,
                "dimensions": m.metadata.get("dimensions", []) if m.metadata else [],
                "score": wire_score(m.score),
                "search_detail": wire_search_detail(m.search_detail),
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
