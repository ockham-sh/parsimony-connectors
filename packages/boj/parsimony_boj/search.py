"""Semantic search over the published Bank of Japan (BoJ) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/boj`` (override with
``PARSIMONY_BOJ_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool are series codes (e.g. ``STRDCLUCON``) or
database identifiers prefixed with ``db:`` (e.g. ``db:FM01``). Pass the
returned ``code`` to :func:`boj_fetch` via its ``code`` parameter (and
``db`` parameter — the ``db`` METADATA column on the catalog row tells you
which DB the code lives in).
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

PARSIMONY_BOJ_CATALOG_URL_ENV = "PARSIMONY_BOJ_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/boj"

_catalog: Catalog | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    async with _catalog_lock:
        if _catalog is None:
            url = os.environ.get(PARSIMONY_BOJ_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
            logger.info("loading BoJ catalog from %s", url)
            _catalog = await Catalog.from_url(url)
    return _catalog


BOJ_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class BojSearchParams(BaseModel):
    """Parameters for :func:`boj_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the BoJ series or database "
                "you want (e.g. 'JPY USD daily exchange rate', 'Japanese "
                "10-year bond yield', 'monetary base'). Bilingual EN/JP "
                "queries both work."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=BOJ_SEARCH_OUTPUT,
    tags=["macro", "jp", "tool"],
)
async def boj_search(params: BojSearchParams) -> pd.DataFrame:
    """Semantic-search the Bank of Japan (BoJ) stat_search catalog.

    Returns the top matching series/DB codes from BoJ's stat_search API
    (49 statistics databases spanning interest rates, financial markets,
    monetary aggregates, balance sheets, prices, public finance, balance
    of payments, BIS, derivatives, TANKAN, flow of funds, and more).

    Pass the returned ``code`` to ``boj_fetch(db=..., code=...)``. Codes
    prefixed ``db:`` identify a whole database (use the suffix as the
    ``db`` parameter); bare codes identify a single series.
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
