"""Semantic search over the published Banque de France (BdF) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/bdf`` (override
with ``PARSIMONY_BDF_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool fall into two families:

* ``"<series_key>"`` — a single series (dot-separated SDMX key, e.g.
  ``"EXR.D.USD.EUR.SP00.A"``). Pass directly to :func:`bdf_fetch` via
  the ``key`` parameter.
* ``"dataset:{dataset_id}"`` — a whole dataset (a coherent bundle of
  series). Strip the ``dataset:`` prefix to get the dataset id; the
  individual series codes inside that dataset can be discovered via
  another search refined on the dataset name.

The catalog row's ``entity_type`` METADATA column makes the prefix
unambiguous for downstream consumers that prefer not to parse the KEY.
The catalog is embedded with a *multilingual* model
(``paraphrase-multilingual-MiniLM-L12-v2``) because BdF metadata is
bilingual (FR + EN) — French queries hit the row directly via the
shared embedding space, not just via subword overlap.
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

PARSIMONY_BDF_CATALOG_URL_ENV = "PARSIMONY_BDF_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/bdf"

_catalog: Catalog | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    async with _catalog_lock:
        if _catalog is None:
            url = os.environ.get(PARSIMONY_BDF_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
            logger.info("loading BdF catalog from %s", url)
            _catalog = await Catalog.from_url(url)
    return _catalog


BDF_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class BdfSearchParams(BaseModel):
    """Parameters for :func:`bdf_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the BdF series or dataset "
                "you want (e.g. 'France inflation rate', 'EUR USD exchange "
                "rate', 'taux de change euro dollar', 'dette publique "
                "française'). French and English queries both work — the "
                "catalog is embedded with a multilingual model."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=BDF_SEARCH_OUTPUT,
    tags=["macro", "fr", "tool"],
)
async def bdf_search(params: BdfSearchParams) -> pd.DataFrame:
    """Semantic-search the Banque de France (BdF) Webstat catalog.

    Returns the top matching series / dataset codes from BdF's Webstat
    database (45 datasets, ~41,607 series spanning exchange rates,
    interest rates, monetary aggregates, balance of payments, French
    public finance, eurozone statistics, and more).

    Pass series codes (dot-separated SDMX keys like
    ``EXR.D.USD.EUR.SP00.A``) directly into :func:`bdf_fetch` via the
    ``key`` parameter. Codes prefixed ``dataset:`` identify whole
    datasets — strip the prefix to use the dataset id for refined
    searches.
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
