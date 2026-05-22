"""Semantic search over the published Reserve Bank of Australia (RBA) catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

RbaSearchParams = CatalogSearchParams

PARSIMONY_RBA_CATALOG_URL_ENV = "PARSIMONY_RBA_CATALOG_URL"

RBA_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

rba_search = make_catalog_search_connector(
    provider="rba",
    default_url="hf://parsimony-dev/rba",
    env_var=PARSIMONY_RBA_CATALOG_URL_ENV,
    tags=["macro", "au", "tool"],
    description=(
        "Semantic-search the Reserve Bank of Australia (RBA) statistical catalog. "
        "Pass the table_id portion (everything before #) to rba_fetch(table_id=...)."
    ),
    output_columns=RBA_SEARCH_OUTPUT.columns,
)
