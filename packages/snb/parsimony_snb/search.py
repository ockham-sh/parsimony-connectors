"""Semantic search over the published Swiss National Bank (SNB) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_snb.catalog_build import build_snb_catalog

SnbSearchParams = CatalogSearchParams

PARSIMONY_SNB_CATALOG_URL_ENV = "PARSIMONY_SNB_CATALOG_URL"

SNB_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

snb_search = make_local_search_connector(
    provider="snb",
    default_url="hf://parsimony-dev/snb",
    catalog_url_env_var=PARSIMONY_SNB_CATALOG_URL_ENV,
    build_catalog=build_snb_catalog,
    tags=["macro", "ch", "tool"],
    description=(
        "Semantic-search the Swiss National Bank (SNB) data portal catalog. "
        "Pass the cube_id portion (everything before #) to snb_fetch(cube_id=...)."
    ),
    output_columns=SNB_SEARCH_OUTPUT.columns,
)
