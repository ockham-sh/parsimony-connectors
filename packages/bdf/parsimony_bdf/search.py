"""Semantic search over the published Banque de France (BdF) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_bdf.catalog_build import build_bdf_catalog

BdfSearchParams = CatalogSearchParams

PARSIMONY_BDF_CATALOG_URL_ENV = "PARSIMONY_BDF_CATALOG_URL"

BDF_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

bdf_search = make_local_search_connector(
    provider="bdf",
    default_url="hf://parsimony-dev/bdf",
    catalog_url_env_var=PARSIMONY_BDF_CATALOG_URL_ENV,
    build_catalog=build_bdf_catalog,
    tags=["macro", "fr", "tool"],
    description=(
        "Search the Banque de France (BdF) Webstat catalog of French "
        "macroeconomic, monetary and financial time series. Returns ranked SDMX "
        "series codes (and dataset:<id> group rows); pass a series code to "
        "bdf_fetch(key=...) to retrieve its observations."
    ),
    output_columns=BDF_SEARCH_OUTPUT.columns,
)

__all__ = ["PARSIMONY_BDF_CATALOG_URL_ENV", "BdfSearchParams", "bdf_search"]
