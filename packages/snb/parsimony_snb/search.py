"""Semantic search over the published Swiss National Bank (SNB) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/snb`` (override with
``PARSIMONY_SNB_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool are compound ``{cube_id}#{series_key}`` keys.
Pass the ``cube_id`` portion (everything before ``#``) to :func:`snb_fetch`
via its ``cube_id`` parameter — SNB fetches an entire cube at once.
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

PARSIMONY_SNB_CATALOG_URL_ENV = "PARSIMONY_SNB_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/snb"

_catalog: Catalog | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    async with _catalog_lock:
        if _catalog is None:
            url = os.environ.get(PARSIMONY_SNB_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
            logger.info("loading SNB catalog from %s", url)
            _catalog = await Catalog.from_url(url)
    return _catalog


SNB_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class SnbSearchParams(BaseModel):
    """Parameters for :func:`snb_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the SNB cube/series you "
                "want (e.g. 'CHF EUR exchange rate', 'Swiss policy rate', "
                "'SNB monetary aggregates M3'). EN/DE/FR/IT all work."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=SNB_SEARCH_OUTPUT,
    tags=["macro", "ch", "tool"],
)
async def snb_search(params: SnbSearchParams) -> pd.DataFrame:
    """Semantic-search the Swiss National Bank (SNB) data portal catalog.

    Covers ~237 SNB cubes across labour market, foreign trade, exchange
    rates, monetary aggregates, banking statistics, financial accounts,
    and more (~4.9k addressable series).

    Pass the ``cube_id`` portion (everything before ``#``) to
    ``snb_fetch(cube_id=...)`` — SNB fetches one whole cube at a time.
    """
    catalog = await _get_catalog()
    matches = await catalog.search(params.query, limit=params.limit)
    return pd.DataFrame(
        [
            {
                "code": m.code,
                "title": m.title,
                "similarity": round(m.similarity, 6),
            }
            for m in matches
        ]
    )
