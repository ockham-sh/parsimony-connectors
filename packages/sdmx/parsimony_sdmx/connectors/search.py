"""``sdmx_series_search`` and ``sdmx_datasets_search`` — MCP-facing tools.

These are the agent's entry point into the SDMX catalog. The MCP server
filters by ``tags=["tool"]``, so any connector here gets exposed to the
LLM as a tool call.

Two-layer caching matches the plan §0 design:

1. **HF disk cache** — :func:`huggingface_hub.snapshot_download` caches
   to ``~/.cache/huggingface/hub/`` automatically. First call to
   ``hf://ockham/sdmx_series_ecb_hicp`` fetches; subsequent calls reuse
   the local cache. ``file://`` URLs skip the download path entirely.
2. **In-process catalog LRU** — once :class:`parsimony.Catalog` is loaded
   from disk, we keep the resolved object in an LRU keyed by namespace.
   FAISS + entries take ~135 MB for an 89k-row catalog so the cap is
   tight; the typical agent walk hits 1-3 catalogs before the user
   moves on.

Catalog URL resolution is driven by ``PARSIMONY_SDMX_CATALOG_ROOT`` —
local testing points it at ``file:///path/to/repo``; production points
it at ``hf://ockham`` (or the deploying org). Either way the per-flow
URL is ``{root}/{namespace}``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import OrderedDict
from typing import Annotated

import pandas as pd
from parsimony.catalog import Catalog
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import DATASETS_NAMESPACE
from parsimony_sdmx.connectors.enumerate_series import series_namespace

logger = logging.getLogger(__name__)

#: Env var carrying the catalog root URL. Examples:
#:
#: - ``file:///home/user/ockham/catalogs/sdmx/repo``
#: - ``hf://ockham`` (canonical production layout)
PARSIMONY_SDMX_CATALOG_ROOT_ENV = "PARSIMONY_SDMX_CATALOG_ROOT"

#: Default LRU size for loaded catalogs. An 89k-row HICP catalog is
#: ~135 MB resident; cap is tight so a chatty agent walk doesn't
#: balloon RAM. Override with ``PARSIMONY_SDMX_CATALOG_LRU_SIZE``.
DEFAULT_LRU_SIZE = 4

_CATALOG_LRU: OrderedDict[str, Catalog] = OrderedDict()
_CATALOG_LRU_LOCK = asyncio.Lock()


# ---------------------------------------------------------------------------
# Catalog URL resolution + LRU
# ---------------------------------------------------------------------------


def _catalog_root() -> str:
    """Read the catalog root from the environment.

    Raises :class:`ProviderError` with an actionable directive if unset —
    the search tools cannot guess where catalogs live, and a silent
    empty-result return would mislead the agent into looking elsewhere.
    """
    root = os.environ.get(PARSIMONY_SDMX_CATALOG_ROOT_ENV, "").strip()
    if not root:
        raise ConnectorError(
            (
                f"Set {PARSIMONY_SDMX_CATALOG_ROOT_ENV} to the catalog root "
                "(e.g. 'file:///path/to/repo' or 'hf://ockham'). "
                "Without it the search tools have nowhere to look. "
                "DO NOT retry — ask the user to set the env var, or stop."
            ),
            provider="sdmx",
        )
    return root.rstrip("/")


def _catalog_url(namespace: str) -> str:
    """Compose ``{root}/{namespace}`` from the env-configured root."""
    return f"{_catalog_root()}/{namespace}"


def _lru_size() -> int:
    raw = os.environ.get("PARSIMONY_SDMX_CATALOG_LRU_SIZE", "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


async def _get_or_load_catalog(namespace: str) -> Catalog:
    """Return a cached :class:`Catalog` for *namespace*, loading on miss.

    The lock guards against two concurrent search calls both racing to
    load the same catalog (small win; mainly prevents the duplicate
    snapshot_download cost on cold cache).
    """
    async with _CATALOG_LRU_LOCK:
        if namespace in _CATALOG_LRU:
            _CATALOG_LRU.move_to_end(namespace)
            return _CATALOG_LRU[namespace]
        url = _catalog_url(namespace)
        logger.info("loading SDMX catalog %s from %s", namespace, url)
        catalog = await Catalog.from_url(url)
        _CATALOG_LRU[namespace] = catalog
        while len(_CATALOG_LRU) > _lru_size():
            evicted, _ = _CATALOG_LRU.popitem(last=False)
            logger.info("LRU evicting SDMX catalog %s", evicted)
        return catalog


def _clear_catalog_lru() -> None:
    """Drop all cached catalogs. Test-only."""
    _CATALOG_LRU.clear()


# ---------------------------------------------------------------------------
# Flow ID parsing — accept several shapes
# ---------------------------------------------------------------------------


def _resolve_series_namespace(flow_id: str) -> str:
    """Map an agent-provided ``flow_id`` to the canonical series namespace.

    Accepts any of:

    * ``"ECB/HICP"``  — agency/flow form (plan §0 example)
    * ``"ECB-HICP"``  — agency-flow dash form
    * ``"sdmx_series_ecb_hicp"`` — full namespace pass-through
    * ``"hicp"`` (plain flow id) — assumes ECB (most common via this MCP)

    A bad shape raises :class:`ProviderError` with the expected forms in
    the message — agents need a directive, not a stack trace.
    """
    raw = flow_id.strip()
    if not raw:
        raise ConnectorError(
            "flow_id must be non-empty (e.g. 'ECB/HICP').",
            provider="sdmx",
        )

    if raw.lower().startswith("sdmx_series_"):
        return raw.lower()

    for sep in ("/", "-"):
        if sep in raw:
            agency_raw, dataset_id = raw.split(sep, 1)
            agency_raw = agency_raw.strip().upper()
            dataset_id = dataset_id.strip()
            if not dataset_id:
                raise ConnectorError(
                    f"flow_id {flow_id!r} missing dataset id after {sep!r}.",
                    provider="sdmx",
                )
            try:
                agency = AgencyId(agency_raw)
            except ValueError:
                raise ConnectorError(
                    (
                        f"Unknown agency {agency_raw!r} in flow_id {flow_id!r}. "
                        f"Supported: {[a.value for a in AgencyId]}."
                    ),
                    provider="sdmx",
                ) from None
            return series_namespace(agency, dataset_id)

    # No separator → single token. Default to ECB (most common for this MCP)
    # but warn so a misconfigured agent surfaces in logs.
    logger.warning(
        "flow_id %r has no agency separator; defaulting to ECB. "
        "Pass 'AGENCY/FLOW' (e.g. 'ESTAT/UNE_RT_M') to disambiguate.",
        flow_id,
    )
    return series_namespace(AgencyId.ECB, raw)


# ---------------------------------------------------------------------------
# Series search
# ---------------------------------------------------------------------------


SERIES_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_key", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
        Column(name="namespace", role=ColumnRole.METADATA),
    ]
)


class SeriesSearchParams(BaseModel):
    """Parameters for :func:`sdmx_series_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the series. "
                "Compose with codelist terms when known "
                "(e.g. 'Spain monthly HICP all-items annual rate of change')."
            ),
        ),
    ]
    flow_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            description=(
                "SDMX flow identifier in 'AGENCY/FLOW' form "
                "(e.g. 'ECB/HICP', 'ESTAT/UNE_RT_M'). "
                "Use sdmx_datasets_search first if unknown."
            ),
        ),
    ]
    limit: int = Field(
        default=10, ge=1, le=50, description="Top-N results to return."
    )


@connector(
    output=SERIES_SEARCH_OUTPUT,
    tags=["sdmx", "tool"],
)
async def sdmx_series_search(params: SeriesSearchParams) -> pd.DataFrame:
    """Hybrid-search a per-flow SDMX series catalog by natural language.

    Loads the published catalog for ``flow_id`` (``hf://`` or
    ``file://`` per ``PARSIMONY_SDMX_CATALOG_ROOT``), runs RRF-fused
    BM25 + FAISS retrieval, returns top-N (series_key, title,
    similarity).

    The LRU caches loaded catalogs so a follow-up search on the same
    flow is in-memory; cold loads from ``hf://`` are network + 100s of
    MB once, then disk-cached by huggingface_hub.

    Agents typically chain: ``sdmx_datasets_search(query) ->
    sdmx_series_search(query, flow_id) -> sdmx_fetch(series_key)``.
    """
    namespace = _resolve_series_namespace(params.flow_id)
    catalog = await _get_or_load_catalog(namespace)
    matches = await catalog.search(params.query, limit=params.limit)

    if not matches:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No matches for query={params.query!r} in flow_id={params.flow_id!r} "
                f"(namespace={namespace}). Try a less specific query, or call "
                f"sdmx_datasets_search to confirm the flow exists."
            ),
        )

    return pd.DataFrame(
        [
            {
                "series_key": m.code,
                "title": m.title,
                "similarity": round(m.similarity, 6),
                "namespace": m.namespace,
            }
            for m in matches
        ]
    )


# ---------------------------------------------------------------------------
# Datasets search
# ---------------------------------------------------------------------------


DATASETS_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="flow_id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
    ]
)


class DatasetsSearchParams(BaseModel):
    """Parameters for :func:`sdmx_datasets_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the dataset/flow you "
                "want (e.g. 'consumer prices', 'unemployment'). "
                "Returns the flow_id you then pass to sdmx_series_search."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50)


@connector(
    output=DATASETS_SEARCH_OUTPUT,
    tags=["sdmx", "tool"],
)
async def sdmx_datasets_search(params: DatasetsSearchParams) -> pd.DataFrame:
    """Discover which SDMX flow to query for a topic.

    Searches the cross-agency :data:`DATASETS_NAMESPACE` catalog. Returns
    one row per matching flow with the canonical ``AGENCY/DATASET_ID``
    form ready to pass to :func:`sdmx_series_search`.
    """
    catalog = await _get_or_load_catalog(DATASETS_NAMESPACE)
    matches = await catalog.search(params.query, limit=params.limit)

    if not matches:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No flow matches for query={params.query!r}. "
                "Try a broader topic word, or list all by agency via "
                "the bulk-fetch enumerate_sdmx_datasets connector."
            ),
        )

    rows: list[dict[str, object]] = []
    for m in matches:
        # The dataset namespace stores rows keyed by the agency-prefixed
        # composite key. metadata may carry agency / dataset_id directly.
        agency = m.metadata.get("agency", "") if m.metadata else ""
        dataset_id = m.metadata.get("dataset_id", "") if m.metadata else ""
        flow_id = f"{agency}/{dataset_id}" if agency and dataset_id else m.code
        rows.append(
            {
                "flow_id": flow_id,
                "title": m.title,
                "similarity": round(m.similarity, 6),
                "agency": agency,
                "dataset_id": dataset_id,
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "DatasetsSearchParams",
    "PARSIMONY_SDMX_CATALOG_ROOT_ENV",
    "SeriesSearchParams",
    "sdmx_datasets_search",
    "sdmx_series_search",
]
