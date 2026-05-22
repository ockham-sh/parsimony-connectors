"""Semantic search over the published US Treasury catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

TreasurySearchParams = CatalogSearchParams

PARSIMONY_TREASURY_CATALOG_URL_ENV = "PARSIMONY_TREASURY_CATALOG_URL"

TREASURY_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

treasury_search = make_catalog_search_connector(
    provider="treasury",
    default_url="hf://parsimony-dev/treasury",
    env_var=PARSIMONY_TREASURY_CATALOG_URL_ENV,
    tags=["macro", "us", "tool"],
    description=(
        "Semantic-search the US Treasury catalog (Fiscal Data + ODM rate feeds). "
        "Dispatch: home/<feed> → treasury_rates_fetch(feed=...); "
        "v<n>/<endpoint>#<field> → treasury_fetch(endpoint=...)."
    ),
    output_columns=TREASURY_SEARCH_OUTPUT.columns,
)
