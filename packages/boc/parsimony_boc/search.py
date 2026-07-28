"""Semantic search over the published Bank of Canada (BoC) catalog."""

from __future__ import annotations

from parsimony.catalog.search import (
    CatalogSearchParams,
    make_local_search_connector,
)
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_boc.catalog_build import build_boc_catalog

PARSIMONY_BOC_CATALOG_URL_ENV = "PARSIMONY_BOC_CATALOG_URL"

BocSearchParams = CatalogSearchParams

BOC_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="group", role=ColumnRole.METADATA),
        Column(name="group_label", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
    ]
)


boc_search = make_local_search_connector(
    provider="boc",
    default_url="hf://parsimony-dev/boc",
    catalog_url_env_var=PARSIMONY_BOC_CATALOG_URL_ENV,
    build_catalog=build_boc_catalog,
    tags=["macro", "ca", "tool"],
    description=(
        "Search the Bank of Canada (BoC) Valet catalog. "
        "query= is literal text; use filter={'code': 'FXUSDCAD'} for an exact id. "
        "Pass returned codes to boc_fetch(series_name=...)."
    ),
    output=BOC_SEARCH_OUTPUT,
)
