"""Semantic search over the published Sveriges Riksbank catalog.

FX/rate row enrichment happens at enumeration time (see ``swea.py``); it is what drives
title/description relevance, so no provider-specific search routing is needed here.
"""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_riksbank.catalog_build import build_riksbank_catalog

RiksbankSearchParams = CatalogSearchParams

PARSIMONY_RIKSBANK_CATALOG_URL_ENV = "PARSIMONY_RIKSBANK_CATALOG_URL"

RIKSBANK_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

# The result ``code`` routes the follow-up fetch by its shape:
#   SWEA series id (e.g. SEKEURPMI)        -> riksbank_fetch(series_id=...)
#   SWESTR id (e.g. SWESTR, SWESTRAVG1M)   -> riksbank_swestr_fetch(series=...)
#   monetary_policy/<id>                   -> riksbank_monetary_policy_fetch(series=<id>)
#   turnover/<market>/<frequency>          -> riksbank_turnover_fetch(market=, frequency=)
#   holdings/<dataset>                     -> riksbank_holdings_fetch(dataset=)
riksbank_search = make_local_search_connector(
    provider="riksbank",
    default_url="hf://parsimony-dev/riksbank",
    catalog_url_env_var=PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    build_catalog=build_riksbank_catalog,
    tags=["macro", "se", "tool"],
    description=(
        "Semantic-search the Sveriges Riksbank catalog across all five products (SWEA "
        "interest & exchange rates, SWESTR, Monetary Policy forecasts, Turnover "
        "statistics, securities Holdings). Route the result `code`: a bare SWESTR id "
        "(SWESTR, SWESTRAVG*) -> riksbank_swestr_fetch(series=...); a bare SWEA id -> "
        "riksbank_fetch(series_id=...); `monetary_policy/<id>` -> "
        "riksbank_monetary_policy_fetch(series=<id>); `turnover/<market>/<frequency>` -> "
        "riksbank_turnover_fetch(market=, frequency=); `holdings/<dataset>` -> "
        "riksbank_holdings_fetch(dataset=<dataset>)."
    ),
    output_columns=RIKSBANK_SEARCH_OUTPUT.columns,
    metadata_columns=("source",),
)

__all__ = [
    "RiksbankSearchParams",
    "PARSIMONY_RIKSBANK_CATALOG_URL_ENV",
    "RIKSBANK_SEARCH_OUTPUT",
    "riksbank_search",
]
