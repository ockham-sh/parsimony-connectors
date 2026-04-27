"""Semantic search over the published Banco de Portugal (BdP) catalog.

Wraps the parquet+FAISS catalog at ``hf://parsimony-dev/bdp`` (override with
``PARSIMONY_BDP_CATALOG_URL`` for local testing) as an MCP tool.

Codes returned by this tool fall into three families:

* ``"{domain_id}:{dataset_id}:{series_id}"`` — a single series. Pass the
  series_id (the trailing segment) into :func:`bdp_fetch` via the
  ``series_ids`` parameter, alongside ``domain_id`` and ``dataset_id``.
* ``"dataset:{domain_id}:{dataset_id}"`` — a whole dataset (a coherent
  bundle of series). Strip the ``dataset:`` prefix and pass the
  ``domain_id`` + ``dataset_id`` segments to :func:`bdp_fetch` to pull
  every series in the dataset.
* ``"domain:{domain_id}"`` — a top-level grouping (e.g. ``National
  financial accounts``). Drill down via the catalog or by listing
  datasets under ``domain_id``.

The catalog row's ``entity_type`` METADATA column makes the prefix
unambiguous for downstream consumers that prefer not to parse the KEY.
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

PARSIMONY_BDP_CATALOG_URL_ENV = "PARSIMONY_BDP_CATALOG_URL"
_DEFAULT_CATALOG_URL = "hf://parsimony-dev/bdp"

_catalog: Catalog | None = None
_catalog_lock = asyncio.Lock()


async def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    async with _catalog_lock:
        if _catalog is None:
            url = os.environ.get(PARSIMONY_BDP_CATALOG_URL_ENV, _DEFAULT_CATALOG_URL)
            logger.info("loading BdP catalog from %s", url)
            _catalog = await Catalog.from_url(url)
    return _catalog


BDP_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="similarity", role=ColumnRole.METADATA),
    ]
)


class BdpSearchParams(BaseModel):
    """Parameters for :func:`bdp_search`."""

    query: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Natural-language description of the BdP series, dataset or "
                "domain you want (e.g. 'Portuguese 10-year government bond "
                "yield', 'household credit transactions', 'balanca de "
                "pagamentos'). Portuguese keywords work via subword overlap "
                "even though the catalog is embedded with an English-only "
                "model."
            ),
        ),
    ]
    limit: int = Field(default=10, ge=1, le=50, description="Top-N results.")


@connector(
    output=BDP_SEARCH_OUTPUT,
    tags=["macro", "pt", "tool"],
)
async def bdp_search(params: BdpSearchParams) -> pd.DataFrame:
    """Semantic-search the Banco de Portugal (BdP) BPstat catalog.

    Returns the top matching series / dataset / domain codes from BdP's
    BPstat database (~65 leaf domains, ~215 datasets, ~72 K series spanning
    national financial accounts, monetary statistics, balance of payments,
    interest rates, securities, and Portuguese non-financial sector data).

    Pass series codes (``"{domain_id}:{dataset_id}:{series_id}"``) into
    :func:`bdp_fetch` after splitting on ``:``. Codes prefixed ``dataset:``
    identify whole datasets (drop the prefix and pass ``domain_id`` +
    ``dataset_id`` to fetch the entire bundle); codes prefixed ``domain:``
    identify a navigation node only.
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
