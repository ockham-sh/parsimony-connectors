"""Semantic search over the published Swiss National Bank (SNB) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_snb.catalog_build import build_snb_catalog

SnbSearchParams = CatalogSearchParams

PARSIMONY_SNB_CATALOG_URL_ENV = "PARSIMONY_SNB_CATALOG_URL"

SNB_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
        Column(name="cube_id", role=ColumnRole.METADATA),
        Column(name="series_key", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
    ]
)

snb_search = make_local_search_connector(
    provider="snb",
    default_url="hf://parsimony-dev/snb",
    catalog_url_env_var=PARSIMONY_SNB_CATALOG_URL_ENV,
    build_catalog=build_snb_catalog,
    tags=["macro", "ch", "tool"],
    description=(
        "Semantic-search the Swiss National Bank (SNB) data portal catalog — both "
        "publication cubes and the SDMX-style data warehouse. Dispatch: "
        "snb_fetch(cube_id=cube_id) using the returned cube_id column directly."
    ),
    output_columns=SNB_SEARCH_OUTPUT.columns,
    metadata_columns=("cube_id", "series_key", "frequency", "category"),
)

__all__ = ["PARSIMONY_SNB_CATALOG_URL_ENV", "SNB_SEARCH_OUTPUT", "SnbSearchParams", "snb_search"]
