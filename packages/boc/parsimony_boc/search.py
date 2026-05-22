"""Semantic search over the published Bank of Canada (BoC) catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

from parsimony_boc.catalog import PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV

BocSearchParams = CatalogSearchParams

PARSIMONY_BOC_CATALOG_URL_ENV = "PARSIMONY_BOC_CATALOG_URL"

BOC_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

boc_search = make_catalog_search_connector(
    provider="boc",
    default_url="hf://parsimony-dev/boc",
    env_var=PARSIMONY_BOC_CATALOG_URL_ENV,
    tags=["macro", "ca", "tool"],
    description=(
        "Search the Bank of Canada (BoC) Valet catalog. "
        "Preferred: structured queries such as 'code: FXUSDCAD'. "
        "Pass returned codes to boc_fetch(series_name=...)."
    ),
    fallback_enumerator=lambda: _enumerate_entries(),
    fallback_env_var=PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV,
    output_columns=BOC_SEARCH_OUTPUT.columns,
)


async def _enumerate_entries():
    from parsimony_boc import enumerate_boc

    result = await enumerate_boc()
    return result.data
