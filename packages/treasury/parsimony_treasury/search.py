"""Semantic search over the published US Treasury catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/treasury`` (override
with ``PARSIMONY_TREASURY_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned dispatch to one of two fetch connectors:

* ``v<n>/<endpoint>#<field>`` (Fiscal Data) → :func:`treasury_fetch` via
  ``endpoint`` (use the part before ``#``); the field name guides which
  measure to read.
* ``home/<feed>`` (Treasury rate XML feeds) → :func:`treasury_rates_fetch`
  via ``feed`` (use the part after ``home/``).
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

PARSIMONY_TREASURY_CATALOG_URL_ENV = "PARSIMONY_TREASURY_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/treasury"

_catalog: Catalog | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    async with _catalog_lock:
        if _catalog is None:
            url = os.environ.get(PARSIMONY_TREASURY_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
            logger.info("loading US Treasury catalog from %s", url)
            _catalog = await Catalog.from_url(url)
    return _catalog


TREASURY_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class TreasurySearchParams(BaseModel):
    """Parameters for :func:`treasury_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the Treasury series you "
                "want (e.g. 'US public debt to the penny', 'daily Treasury "
                "yield curve 10-year', 'monthly Treasury statement receipts')."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=TREASURY_SEARCH_OUTPUT,
    tags=["macro", "us", "tool"],
)
async def treasury_search(params: TreasurySearchParams) -> pd.DataFrame:
    """Semantic-search the US Treasury catalog (Fiscal Data + ODM rate feeds).

    Covers ~884 Fiscal Data measure fields across debt, federal accounts,
    monthly Treasury statements, etc., plus ~35 entries from the 5
    daily Treasury rate feeds (yield curve, real yield curve, bill rates,
    long-term rate, real long-term rate).

    Dispatch by code prefix: ``home/<feed>`` →
    ``treasury_rates_fetch(feed=...)``; ``v<n>/<endpoint>#<field>`` →
    ``treasury_fetch(endpoint=...)``.
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
