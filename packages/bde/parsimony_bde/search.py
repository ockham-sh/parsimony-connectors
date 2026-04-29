"""Semantic search over the published Banco de España (BdE) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/bde`` (override with
``PARSIMONY_BDE_CATALOG_URL`` for local testing) as an MCP tool. The agent
calls :func:`bde_search` with a natural-language query and gets back the
top-N matches with their codes, titles, and similarity scores.

Codes returned by this tool are ``serie`` IDs that :func:`bde_fetch`
accepts directly via its ``key`` parameter — the discover→fetch handshake.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import pandas as pd
from parsimony.catalog import Catalog, CatalogCache
from parsimony.connector import connector
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

#: Env var carrying the BdE catalog URL. Defaults to the canonical HF repo.
#: Override with e.g. ``file:///path/to/catalogs/bde/repo/bde`` for local testing.
PARSIMONY_BDE_CATALOG_URL_ENV = "PARSIMONY_BDE_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/bde"

# Single-catalog cache. The BdE catalog is ~26 MB FAISS + 0.6 MB parquet —
# a one-time load amortizes across every search call in the MCP session.
_CATALOG_CACHE = CatalogCache(max_size=1)


async def _get_catalog() -> Catalog:
    """Return the singleton BdE catalog, loading from URL on first use."""
    url = os.environ.get(PARSIMONY_BDE_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
    return await _CATALOG_CACHE.get(url)


BDE_SEARCH_OUTPUT = OutputConfig(
    columns=[
        # ``code`` is the serie ID (e.g. ``D_1NBAF472``) that bde_fetch
        # accepts via its ``key`` parameter — the search→fetch handshake.
        Column(name="code", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class BdeSearchParams(BaseModel):
    """Parameters for :func:`bde_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the BdE series you want "
                "(e.g. 'Spanish 10-year bond yield', 'Euribor 3-month', "
                "'monthly HICP Spain'). Spanish or English both work."
            ),
        ),
    ]
    limit: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Top-N results to return.",
    )


@connector(
    output=BDE_SEARCH_OUTPUT,
    tags=["macro", "es", "tool"],
)
async def bde_search(params: BdeSearchParams) -> pd.DataFrame:
    """Semantic-search the Banco de España (BdE) catalog by natural language.

    Returns the top matching ``serie`` codes from BdE's published
    statistical catalog (~15.5k unique series across 7 chapters: general
    statistics, financial accounts, international economy, bank lending
    survey, financial indicators, exchange rates, interest rates).

    Pass the returned ``code`` to ``bde_fetch(key=...)`` to retrieve the
    actual time series. The catalog is bilingual — both Spanish and English
    queries route correctly through the embedder.
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
