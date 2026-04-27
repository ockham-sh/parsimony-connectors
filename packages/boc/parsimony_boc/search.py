"""Semantic search over the published Bank of Canada (BoC) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/boc`` (override with
``PARSIMONY_BOC_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool are series names (e.g. ``FXUSDCAD``) or group
identifiers prefixed with ``group:`` (e.g. ``group:FX_RATES_DAILY``). Pass
the returned ``code`` to :func:`boc_fetch` via its ``series_name`` parameter.
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

PARSIMONY_BOC_CATALOG_URL_ENV = "PARSIMONY_BOC_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/boc"

_catalog: Catalog | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    async with _catalog_lock:
        if _catalog is None:
            url = os.environ.get(PARSIMONY_BOC_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
            logger.info("loading BoC catalog from %s", url)
            _catalog = await Catalog.from_url(url)
    return _catalog


BOC_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class BocSearchParams(BaseModel):
    """Parameters for :func:`boc_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the BoC series or group "
                "you want (e.g. 'CAD USD daily exchange rate', 'Canadian "
                "10-year bond yield', 'overnight rate'). Bilingual EN/FR "
                "queries both work."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=BOC_SEARCH_OUTPUT,
    tags=["macro", "ca", "tool"],
)
async def boc_search(params: BocSearchParams) -> pd.DataFrame:
    """Semantic-search the Bank of Canada (BoC) Valet catalog.

    Returns the top matching series/group codes from BoC's Valet API
    (~15.4k series + 2.4k groups across exchange rates, interest rates,
    monetary aggregates, balance sheets, FMI surveys, and more).

    Pass the returned ``code`` to ``boc_fetch(series_name=...)``. Codes
    prefixed ``group:`` resolve to the full group panel; bare codes
    resolve to a single series.
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
