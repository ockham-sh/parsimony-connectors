"""Semantic search over the published EIA dataset catalog."""

from __future__ import annotations

from parsimony.catalog.search import (
    CatalogSearchParams,
    make_local_search_connector,
)
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_eia.catalog_build import build_eia_catalog

EiaSearchParams = CatalogSearchParams

PARSIMONY_EIA_CATALOG_URL_ENV = "PARSIMONY_EIA_CATALOG_URL"

EIA_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequencies", role=ColumnRole.METADATA),
        Column(name="default_frequency", role=ColumnRole.METADATA),
        Column(name="units", role=ColumnRole.METADATA),
    ]
)

eia_search = make_local_search_connector(
    provider="eia",
    default_url="hf://parsimony-dev/eia",
    catalog_url_env_var=PARSIMONY_EIA_CATALOG_URL_ENV,
    build_catalog=build_eia_catalog,
    tags=["macro", "energy", "us", "tool"],
    description=(
        "Search the U.S. Energy Information Administration (EIA) catalog of energy "
        "datasets: petroleum, natural gas, electricity, coal, nuclear, renewables, emissions. "
        "Each hit's code is a dataset route for eia_fetch; call eia_facets(route, facet) "
        "for facet values, then eia_fetch. eia_fetch_series handles legacy ids like PET.RWTC.D."
    ),
    output=EIA_SEARCH_OUTPUT,
)

__all__ = ["PARSIMONY_EIA_CATALOG_URL_ENV", "EiaSearchParams", "eia_search"]
