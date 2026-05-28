"""Semantic search over the published Reserve Bank of Australia (RBA) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_rba.catalog_build import build_rba_catalog

RbaSearchParams = CatalogSearchParams

PARSIMONY_RBA_CATALOG_URL_ENV = "PARSIMONY_RBA_CATALOG_URL"

RBA_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

rba_search = make_local_search_connector(
    provider="rba",
    default_url="hf://parsimony-dev/rba",
    catalog_url_env_var=PARSIMONY_RBA_CATALOG_URL_ENV,
    build_catalog=build_rba_catalog,
    tags=["macro", "au", "tool"],
    description=(
        "Semantic-search the Reserve Bank of Australia (RBA) statistical catalog. "
        "Pass the table_id portion (everything before #) to rba_fetch(table_id=...)."
    ),
    output_columns=RBA_SEARCH_OUTPUT.columns,
)
