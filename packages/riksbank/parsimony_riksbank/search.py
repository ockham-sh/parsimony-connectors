"""Semantic search over the published Sveriges Riksbank catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/riksbank`` (override
with ``PARSIMONY_RIKSBANK_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned are SWEA ``series_id`` (e.g. ``SECBREPOEFF``) or SWESTR
identifiers (e.g. ``SWESTR``, ``SWESTRAVG1M``, ``SWESTRINDEX``). Dispatch:

* SWEA → :func:`riksbank_fetch` via ``series_id``
* SWESTR → :func:`riksbank_swestr_fetch` via ``series``

The dispatch hint lives in the catalog row's ``source`` metadata
(``"swea"`` vs ``"swestr"``); only ``code``+``title``+``score`` are
returned by this tool to keep the discovery surface compact.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Annotated

import pandas as pd
from parsimony.catalog import Catalog
from parsimony.connector import connector
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

PARSIMONY_RIKSBANK_CATALOG_URL_ENV = "PARSIMONY_RIKSBANK_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/riksbank"

_catalog: Catalog | None = None
_catalog_url: str | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog(catalog_url: str | None = None) -> Catalog:
    global _catalog, _catalog_url
    url = catalog_url or os.environ.get(PARSIMONY_RIKSBANK_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
    async with _catalog_lock:
        if _catalog is None or _catalog_url != url:
            _catalog = await Catalog.load(url)
            _catalog_url = url
        return _catalog


RIKSBANK_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)


class RiksbankSearchParams(BaseModel):
    """Parameters for :func:`riksbank_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the Riksbank series you "
                "want (e.g. 'Swedish policy rate', 'SEK USD exchange rate', "
                "'SWESTR overnight rate', 'SWESTR 1-month average')."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")
    catalog_url: str | None = Field(default=None, description="Override catalog URL, e.g. file:///tmp/riksbank.")


@connector(
    output=RIKSBANK_SEARCH_OUTPUT,
    tags=["macro", "se", "tool"],
)
async def riksbank_search(query: str, limit: int = 10, catalog_url: str | None = None) -> pd.DataFrame:
    """Semantic-search the Sveriges Riksbank catalog.

    Covers SWEA (interest rates and exchange rates, ~117 series) plus
    SWESTR (the Swedish Krona Short-Term Rate, its compounded averages
    1W/1M/2M/3M/6M, and the SWESTR index).

    Dispatch by code: SWESTR identifiers (``SWESTR``, ``SWESTRAVG*``,
    ``SWESTRINDEX``) → ``riksbank_swestr_fetch(series=...)``; everything
    else (SWEA) → ``riksbank_fetch(series_id=...)``.
    """
    params = RiksbankSearchParams(query=query, limit=limit, catalog_url=catalog_url)
    catalog = await _get_catalog(params.catalog_url)
    matches = await catalog.search(params.query, limit=params.limit)
    return pd.DataFrame(
        [
            {
                "code": m.code,
                "title": m.title,
                "score": round(m.score, 6),
            }
            for m in matches
        ]
    )
