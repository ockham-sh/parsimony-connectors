"""Semantic search over the published Bank of Canada (BoC) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/boc`` (override with
``PARSIMONY_BOC_CATALOG_URL`` for local testing) as an MCP tool.

When ``fallback_bm25=True`` (or ``PARSIMONY_BOC_CATALOG_FALLBACK_BM25=1``),
a missing published snapshot triggers a one-time live enumeration and an
in-process BM25 catalog build so search remains usable during catalog dev.

Codes returned by this tool are series names (e.g. ``FXUSDCAD``) or group
identifiers prefixed with ``group:`` (e.g. ``group:FX_RATES_DAILY``). Pass
the returned ``code`` to :func:`boc_fetch` via its ``series_name`` parameter.
"""

from __future__ import annotations

import logging
from typing import Annotated

import pandas as pd
from parsimony.connector import connector
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_boc.catalog import (
    PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV,
    get_catalog,
)

logger = logging.getLogger(__name__)

PARSIMONY_BOC_CATALOG_URL_ENV = "PARSIMONY_BOC_CATALOG_URL"


BOC_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
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
                "Structured field query (preferred), e.g. 'code: FXUSDCAD' or "
                "'code: group:FX_RATES_DAILY'. Plain text without field syntax "
                "searches title only (BM25 broad fallback). Bilingual EN/FR."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")
    catalog_url: str | None = Field(default=None, description="Override catalog URL, e.g. file:///tmp/boc.")
    fallback_bm25: bool = Field(
        default=False,
        description=(
            "When true, build a local BM25 catalog from live enumeration if the "
            f"published snapshot is missing. Also enabled by {PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV}=1."
        ),
    )


async def _get_catalog_for_search(params: BocSearchParams):
    from parsimony_boc import BocEnumerateParams, enumerate_boc

    return await get_catalog(
        catalog_url=params.catalog_url,
        fallback_bm25=params.fallback_bm25,
        enumerate=lambda: enumerate_boc(BocEnumerateParams()),
    )


@connector(
    output=BOC_SEARCH_OUTPUT,
    tags=["macro", "ca", "tool"],
)
async def boc_search(params: BocSearchParams) -> pd.DataFrame:
    """Search the Bank of Canada (BoC) Valet catalog.

    **Preferred:** structured queries such as ``code: FXUSDCAD`` or
    ``code: group:FX_RATES_DAILY``. Plain text without field syntax
    searches the ``title`` index only (BM25 broad fallback).

    Returns the top matching series/group codes from BoC's Valet API
    (~15.4k series + 2.4k groups across exchange rates, interest rates,
    monetary aggregates, balance sheets, FMI surveys, and more).

    Pass the returned ``code`` to ``boc_fetch(series_name=...)``. Codes
    prefixed ``group:`` resolve to the full group panel; bare codes
    resolve to a single series.
    """
    catalog = await _get_catalog_for_search(params)
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
