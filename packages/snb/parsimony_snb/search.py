"""Semantic search over the published Swiss National Bank (SNB) catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

SnbSearchParams = CatalogSearchParams

PARSIMONY_SNB_CATALOG_URL_ENV = "PARSIMONY_SNB_CATALOG_URL"

SNB_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

snb_search = make_catalog_search_connector(
    provider="snb",
    default_url="hf://parsimony-dev/snb",
    env_var=PARSIMONY_SNB_CATALOG_URL_ENV,
    tags=["macro", "ch", "tool"],
    description=(
        "Semantic-search the Swiss National Bank (SNB) data portal catalog. "
        "Pass the cube_id portion (everything before #) to snb_fetch(cube_id=...)."
    ),
    output_columns=SNB_SEARCH_OUTPUT.columns,
)
