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
        Column(name="table_id", role=ColumnRole.METADATA),
        Column(name="series_id", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
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
        "Dispatch: rba_fetch(table_id=table_id); filter result by series_id column."
    ),
    output_columns=RBA_SEARCH_OUTPUT.columns,
    metadata_columns=("table_id", "series_id", "frequency", "category", "unit"),
)
