"""Semantic search over the published Banco de España (BdE) catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

BdeSearchParams = CatalogSearchParams

PARSIMONY_BDE_CATALOG_URL_ENV = "PARSIMONY_BDE_CATALOG_URL"

BDE_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

bde_search = make_catalog_search_connector(
    provider="bde",
    default_url="hf://parsimony-dev/bde",
    env_var=PARSIMONY_BDE_CATALOG_URL_ENV,
    tags=["macro", "es", "tool"],
    description=(
        "Semantic-search the Banco de España (BdE) catalog. "
        "Pass returned serie code to bde_fetch(key=...)."
    ),
    output_columns=BDE_SEARCH_OUTPUT.columns,
)
