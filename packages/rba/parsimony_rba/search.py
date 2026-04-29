"""Semantic search over the published Reserve Bank of Australia (RBA) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/rba`` (override with
``PARSIMONY_RBA_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool are compound ``{table_id}#{series_id}`` keys.
Pass the ``table_id`` portion (everything before ``#``) to :func:`rba_fetch`
via its ``table_id`` parameter — RBA fetches an entire table at once.
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

PARSIMONY_RBA_CATALOG_URL_ENV = "PARSIMONY_RBA_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/rba"

_CATALOG_CACHE = CatalogCache(max_size=1)


async def _get_catalog() -> Catalog:
    url = os.environ.get(PARSIMONY_RBA_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
    return await _CATALOG_CACHE.get(url)


RBA_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class RbaSearchParams(BaseModel):
    """Parameters for :func:`rba_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the RBA series you want "
                "(e.g. 'Australian cash rate target', 'AUD USD exchange "
                "rate', 'CPI inflation Australia')."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=RBA_SEARCH_OUTPUT,
    tags=["macro", "au", "tool"],
)
async def rba_search(params: RbaSearchParams) -> pd.DataFrame:
    """Semantic-search the Reserve Bank of Australia (RBA) statistical catalog.

    Returns the top matching ``{table_id}#{series_id}`` codes from RBA's
    statistical tables (~4.7k series across CSV tables, XLSX-exclusive
    sheets, and historical xls binaries — covering cash rate, exchange
    rates, banking, monetary aggregates, payments, etc.).

    Pass the ``table_id`` portion (everything before ``#``) to
    ``rba_fetch(table_id=...)`` — RBA fetches one whole table at a time.
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
