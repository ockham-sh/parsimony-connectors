"""Semantic search over the published Banque de France (BdF) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_bdf.catalog_build import build_bdf_catalog

BdfSearchParams = CatalogSearchParams

PARSIMONY_BDF_CATALOG_URL_ENV = "PARSIMONY_BDF_CATALOG_URL"

BDF_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

bdf_search = make_local_search_connector(
    provider="bdf",
    default_url="hf://parsimony-dev/bdf",
    catalog_url_env_var=PARSIMONY_BDF_CATALOG_URL_ENV,
    build_catalog=build_bdf_catalog,
    tags=["macro", "fr", "tool"],
    description=(
        "Semantic-search the Banque de France (BdF) Webstat catalog. "
        "Pass series codes (dot-separated SDMX keys) to bdf_fetch(key=...)."
    ),
    output_columns=BDF_SEARCH_OUTPUT.columns,
)
