"""Semantic search over the published Banco de España (BdE) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_bde.catalog_build import build_bde_catalog

BdeSearchParams = CatalogSearchParams

PARSIMONY_BDE_CATALOG_URL_ENV = "PARSIMONY_BDE_CATALOG_URL"

BDE_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

bde_search = make_local_search_connector(
    provider="bde",
    default_url="hf://parsimony-dev/bde",
    catalog_url_env_var=PARSIMONY_BDE_CATALOG_URL_ENV,
    build_catalog=build_bde_catalog,
    tags=["macro", "es", "tool"],
    description=(
        "Semantic-search the Banco de España (BdE) catalog. "
        "Pass returned serie code to bde_fetch(key=...)."
    ),
    output_columns=BDE_SEARCH_OUTPUT.columns,
)
