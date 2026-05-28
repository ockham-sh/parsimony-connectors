"""Semantic search over the published US Treasury catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_treasury.catalog_build import build_treasury_catalog

TreasurySearchParams = CatalogSearchParams

PARSIMONY_TREASURY_CATALOG_URL_ENV = "PARSIMONY_TREASURY_CATALOG_URL"

TREASURY_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

treasury_search = make_local_search_connector(
    provider="treasury",
    default_url="hf://parsimony-dev/treasury",
    catalog_url_env_var=PARSIMONY_TREASURY_CATALOG_URL_ENV,
    build_catalog=build_treasury_catalog,
    tags=["macro", "us", "tool"],
    description=(
        "Semantic-search the US Treasury catalog (Fiscal Data + ODM rate feeds). "
        "Dispatch: home/<feed> → treasury_rates_fetch(feed=...); "
        "v<n>/<endpoint>#<field> → treasury_fetch(endpoint=...)."
    ),
    output_columns=TREASURY_SEARCH_OUTPUT.columns,
)
