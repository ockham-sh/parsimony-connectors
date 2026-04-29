"""``sdmx_series_search`` and ``sdmx_datasets_search`` — MCP-facing tools.

These are the agent's entry point into the SDMX catalog. The MCP server
filters by ``tags=["tool"]``, so any connector here gets exposed to the
LLM as a tool call.

Catalogs live as namespace subfolders in a single multi-bundle HF
dataset repo at :data:`DEFAULT_CATALOG_ROOT` — override the root for
local dev with the ``PARSIMONY_SDMX_CATALOG_URL`` env var (matches the
peer-connector convention). The kernel's ``Catalog.from_url``
understands ``hf://<org>/<repo>/<sub>`` and fetches only the requested
bundle via ``snapshot_download(allow_patterns=...)``, so cold-start
cost is bounded to the namespace the agent actually queried (~14 MB
for the cross-agency dataset index, 50-300 MB per series flow).
Two-layer caching keeps the steady state cheap:

1. **HF disk cache** — ``snapshot_download`` caches under
   ``~/.cache/huggingface/hub/`` automatically. Second call to the
   same bundle is a no-op fetch.
2. **In-process catalog LRU** — once a namespace's :class:`Catalog`
   is loaded, it stays resident in an LRU keyed by namespace. FAISS +
   entries take ~135 MB for an 89k-row catalog so the cap is tight;
   the typical agent walk hits 1-3 catalogs before the user moves on.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import pandas as pd
from huggingface_hub.errors import RepositoryNotFoundError
from parsimony.catalog import Catalog, CatalogCache
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import DATASETS_NAMESPACE
from parsimony_sdmx.connectors.enumerate_series import series_namespace

logger = logging.getLogger(__name__)

#: Override env var for the catalog root. Same naming convention as
#: peer connectors (``PARSIMONY_<X>_CATALOG_URL``). Useful for pointing
#: at a local snapshot during catalog dev (``file:///abs/path``).
PARSIMONY_SDMX_CATALOG_URL_ENV = "PARSIMONY_SDMX_CATALOG_URL"

#: Default catalog root: a single multi-bundle HF dataset repo holding
#: every namespace as a subfolder.
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/sdmx"

#: How many hydrated bundles to keep resident at once. An 89k-row HICP
#: catalog is ~135 MB so the cap is tight; the typical agent walk hits
#: 1-3 namespaces before moving on. Override at process start with
#: ``PARSIMONY_SDMX_CATALOG_LRU_SIZE``.
DEFAULT_LRU_SIZE = 4


def _lru_size_from_env() -> int:
    raw = os.environ.get("PARSIMONY_SDMX_CATALOG_LRU_SIZE", "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


_CATALOG_CACHE = CatalogCache(max_size=_lru_size_from_env())


# ---------------------------------------------------------------------------
# Per-namespace catalog loading (delegates to kernel CatalogCache + sub-path)
# ---------------------------------------------------------------------------


def _catalog_root() -> str:
    """Resolve the catalog root: env override or :data:`DEFAULT_CATALOG_ROOT`."""
    return os.environ.get(PARSIMONY_SDMX_CATALOG_URL_ENV, DEFAULT_CATALOG_ROOT).rstrip("/")


async def _get_or_load_catalog(namespace: str) -> Catalog:
    """Return a cached :class:`Catalog` for *namespace*, loading on miss.

    Wraps :class:`~parsimony.catalog.CatalogCache.get` to translate the
    kernel's raw ``RepositoryNotFoundError`` / ``FileNotFoundError``
    into a directive-bearing :class:`ConnectorError` — agents need a
    recovery hint, not a stack trace.
    """
    url = f"{_catalog_root()}/{namespace}"
    try:
        return await _CATALOG_CACHE.get(url)
    except RepositoryNotFoundError as exc:
        raise ConnectorError(
            (
                f"SDMX catalog repo for {namespace!r} not found at {url}. "
                "The bundle has not been published. Try sdmx_datasets_search "
                "to confirm flow_id, or pick a published flow. DO NOT retry."
            ),
            provider="sdmx",
        ) from exc
    except FileNotFoundError as exc:
        raise ConnectorError(
            (
                f"SDMX bundle for {namespace!r} not present at {url}. "
                "The namespace exists in the repo but its meta.json is "
                "missing. DO NOT retry."
            ),
            provider="sdmx",
        ) from exc


def _clear_catalog_lru() -> None:
    """Drop all cached catalogs. Test-only."""
    _CATALOG_CACHE.clear()


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

    Loads the bundle for ``flow_id`` from :data:`DEFAULT_CATALOG_ROOT`
    (a multi-bundle HF dataset repo) and runs RRF-fused BM25 + FAISS
    retrieval, returning top-N (series_key, title, similarity).

    The LRU caches loaded catalogs so a follow-up search on the same
    flow is in-memory; cold loads fetch only the namespace's subfolder
    (50-300 MB once), then disk-cached by huggingface_hub. Unpublished
    flows raise :class:`ConnectorError` with a recovery directive.

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
    "DEFAULT_CATALOG_ROOT",
    "DatasetsSearchParams",
    "PARSIMONY_SDMX_CATALOG_URL_ENV",
    "SeriesSearchParams",
    "sdmx_datasets_search",
    "sdmx_series_search",
]
