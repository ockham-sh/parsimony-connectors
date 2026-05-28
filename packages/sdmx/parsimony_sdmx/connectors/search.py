"""``sdmx_series_search`` and ``sdmx_datasets_search`` — MCP-facing tools.

**Expected usage (structured search first):**

1. ``sdmx_datasets_search(agency=..., query=...)`` on ``sdmx_datasets_<agency>``.
2. Read ``dimensions`` for flows with published series catalogs.
3. ``sdmx_series_search(flow_id='AGENCY/FLOW', ...)``.
4. ``sdmx_fetch``.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import pandas as pd
from parsimony.catalog import Catalog
from parsimony.catalog.search import CatalogLRU, resolved_catalog_url
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_sdmx.catalog_build import build_agency_datasets_catalog, build_series_catalog
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import datasets_namespace
from parsimony_sdmx.connectors.enumerate_series import series_namespace

logger = logging.getLogger(__name__)

PARSIMONY_SDMX_CATALOG_URL_ENV = "PARSIMONY_SDMX_CATALOG_URL"
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/sdmx"
DEFAULT_LRU_SIZE = 4


def _lru_size_from_env() -> int:
    raw = os.environ.get("PARSIMONY_SDMX_CATALOG_LRU_SIZE", "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


_lru = CatalogLRU(_lru_size_from_env())


async def _get_or_load_catalog(
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
    return await _lru.get_or_load(url, cache_path=cache_path, build=build)


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


def _resolve_datasets_namespace(*, agency: AgencyId | str | None) -> str:
    """Map *agency* to ``sdmx_datasets_<agency>`` (agency is required)."""

    if agency is None:
        raise ConnectorError(
            "sdmx_datasets_search requires agency (e.g. agency='ECB'). DO NOT retry without it.",
            provider="sdmx",
        )
    parsed = _parse_agency(agency) if isinstance(agency, str) else agency
    return datasets_namespace(parsed)


def _parse_series_flow(flow_id: str) -> tuple[AgencyId, str]:
    """Return ``(agency, dataset_id)`` from ``AGENCY/FLOW`` or a series namespace."""
    raw = flow_id.strip()
    if not raw:
        raise ConnectorError(
            "flow_id must be non-empty in 'AGENCY/FLOW' form (e.g. 'ECB/HICP').",
            provider="sdmx",
        )
    if raw.lower().startswith("sdmx_series_"):
        token = raw.lower().removeprefix("sdmx_series_")
        for agency in AgencyId:
            prefix = f"{agency.value.lower()}_"
            if token.startswith(prefix):
                return agency, token[len(prefix) :]
        raise ConnectorError(f"Could not parse series namespace {flow_id!r}.", provider="sdmx")
    for sep in ("/", "-"):
        if sep in raw:
            agency_raw, dataset_id = raw.split(sep, 1)
            dataset_id = dataset_id.strip()
            if not dataset_id:
                raise ConnectorError(
                    f"flow_id {flow_id!r} missing dataset id after {sep!r}.",
                    provider="sdmx",
                )
            return _parse_agency(agency_raw), dataset_id
    raise ConnectorError(
        f"flow_id {flow_id!r} must include agency and dataset (e.g. 'ECB/HICP').",
        provider="sdmx",
    )


def _resolve_series_namespace(flow_id: str) -> str:
    """Map ``AGENCY/FLOW`` (or namespace pass-through) to a series catalog namespace."""
    agency, dataset_id = _parse_series_flow(flow_id)
    return series_namespace(agency, dataset_id)


SERIES_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_key", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
        Column(name="namespace", role=ColumnRole.METADATA),
    ]
)


class SeriesSearchParams(BaseModel):
    query: Annotated[str, Field(min_length=1, max_length=512)]
    flow_id: Annotated[str, Field(min_length=1, max_length=128)]
    limit: int = Field(default=10, ge=1, le=50)
    catalog_root: str | None = None


@connector(output=SERIES_SEARCH_OUTPUT, tags=["sdmx", "tool"])
async def sdmx_series_search(
    query: str,
    flow_id: str,
    limit: int = 10,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Search a per-flow SDMX series catalog (structured queries preferred).

    ``flow_id`` must be ``AGENCY/FLOW`` (e.g. ``ECB/YC``). Chain:
    ``sdmx_datasets_search(agency=...)`` → ``sdmx_series_search`` → ``sdmx_fetch``.
    """
    params = SeriesSearchParams(query=query, flow_id=flow_id, limit=limit, catalog_root=catalog_root)
    agency, dataset_id = _parse_series_flow(params.flow_id)
    namespace = series_namespace(agency, dataset_id)

    async def _build() -> Catalog:
        return await build_series_catalog(agency, dataset_id)

    catalog = await _get_or_load_catalog(namespace, catalog_root=params.catalog_root, build=_build)
    matches, _ = await catalog.search(params.query, limit=params.limit)

    if not matches:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No matches for query={params.query!r} in flow_id={params.flow_id!r} "
                f"(namespace={namespace}). Try sdmx_datasets_search first."
            ),
        )

    return pd.DataFrame(
        [
            {
                "series_key": m.code,
                "title": m.title,
                "score": round(m.score, 6),
                "namespace": m.namespace,
            }
            for m in matches
        ]
    )


DATASETS_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="flow_id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(
            name="dimensions",
            role=ColumnRole.METADATA,
            description="Dimension manifest when a series catalog exists for this flow.",
        ),
    ]
)


class DatasetsSearchParams(BaseModel):
    query: Annotated[str, Field(min_length=1, max_length=512)]
    agency: Annotated[str, Field(min_length=1, max_length=32, description="SDMX agency, e.g. ECB.")]
    limit: int = Field(default=10, ge=1, le=50)
    catalog_root: str | None = None


@connector(output=DATASETS_SEARCH_OUTPUT, tags=["sdmx", "tool"])
async def sdmx_datasets_search(
    query: str,
    agency: str,
    limit: int = 10,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Discover SDMX flows within one agency's dataset catalog.

    ``agency`` is required (e.g. ``ECB``). Searches ``sdmx_datasets_<agency>``.
    Use structured ``code: AGENCY|FLOW`` or plain title text. Returns ``dimensions``
    only for flows with a published series catalog.
    """
    params = DatasetsSearchParams(query=query, agency=agency, limit=limit, catalog_root=catalog_root)
    parsed_agency = _parse_agency(params.agency)
    namespace = datasets_namespace(parsed_agency)

    async def _build() -> Catalog:
        return await build_agency_datasets_catalog(parsed_agency)

    catalog = await _get_or_load_catalog(namespace, catalog_root=params.catalog_root, build=_build)
    matches, _ = await catalog.search(params.query, limit=params.limit)

    if not matches:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No flow matches for query={params.query!r} in agency={params.agency!r}. "
                "Try a broader title query or code: AGENCY|FLOW."
            ),
        )

    rows: list[dict[str, object]] = []
    for m in matches:
        row_agency = str(m.metadata.get("agency", "") if m.metadata else "")
        dataset_id = str(m.metadata.get("dataset_id", "") if m.metadata else "")
        flow_id = f"{row_agency}/{dataset_id}" if row_agency and dataset_id else m.code
        dimensions = m.metadata.get("dimensions", []) if m.metadata else []
        if not isinstance(dimensions, list):
            dimensions = []
        rows.append(
            {
                "flow_id": flow_id,
                "title": m.title,
                "score": round(m.score, 6),
                "agency": row_agency,
                "dataset_id": dataset_id,
                "dimensions": dimensions,
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "DEFAULT_CATALOG_ROOT",
    "DatasetsSearchParams",
    "PARSIMONY_SDMX_CATALOG_URL_ENV",
    "SeriesSearchParams",
    "_clear_catalog_lru",
    "_resolve_datasets_namespace",
    "_resolve_series_namespace",
    "sdmx_datasets_search",
    "sdmx_series_search",
    "set_catalog_lru_size",
]
