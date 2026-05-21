"""Semantic search over the published Destatis catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/destatis`` (override
with ``PARSIMONY_DESTATIS_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool are either statistic codes (e.g. ``81000`` —
National Accounts) or table codes (e.g. ``61111-0001`` — CPI). Pass the
returned ``code`` to :func:`destatis_fetch` via its ``name`` parameter; the
``entity_type`` METADATA column on the catalog row tells you whether it is a
statistic-level grouping or a directly-fetchable table.
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

PARSIMONY_DESTATIS_CATALOG_URL_ENV = "PARSIMONY_DESTATIS_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/destatis"

_catalog: Catalog | None = None
_catalog_url: str | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog(catalog_url: str | None = None) -> Catalog:
    global _catalog, _catalog_url
    url = catalog_url or os.environ.get(PARSIMONY_DESTATIS_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
    async with _catalog_lock:
        if _catalog is None or _catalog_url != url:
            _catalog = await Catalog.load(url)
            _catalog_url = url
        return _catalog


DESTATIS_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)


class DestatisSearchParams(BaseModel):
    """Parameters for :func:`destatis_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description (DE or EN both work) of the "
                "Destatis statistic or table you want — e.g. "
                "'Verbraucherpreisindex', 'consumer price index', "
                "'BIP nominal', 'employment by sector'. The catalog is "
                "embedded with paraphrase-multilingual-MiniLM-L12-v2 so "
                "German and English queries hit the same entries."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")
    catalog_url: str | None = Field(default=None, description="Override catalog URL, e.g. file:///tmp/destatis.")


@connector(
    output=DESTATIS_SEARCH_OUTPUT,
    tags=["macro", "de", "tool"],
)
async def destatis_search(query: str, limit: int = 10, catalog_url: str | None = None) -> pd.DataFrame:
    """Semantic-search the German Federal Statistical Office (Destatis) catalog.

    Returns the top matching statistic / table codes from Destatis'
    GENESIS-Online database (~331 statistics + ~2,999 tables spanning
    prices, national accounts, labor, demographics, finance, environment,
    health, education, transport, foreign trade, and more).

    Pass the returned ``code`` to ``destatis_fetch(name=...)``. Codes
    containing a hyphen identify a single fetchable table (e.g.
    ``61111-0001`` — CPI); bare numeric codes (e.g. ``81000``) identify a
    whole statistic (a grouping of tables — drill into it via the catalog
    or via ``parent_statistic`` on table rows).
    """
    params = DestatisSearchParams(query=query, limit=limit, catalog_url=catalog_url)
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
