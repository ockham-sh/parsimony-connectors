"""Semantic search over the published EIA dataset catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_eia.catalog_build import build_eia_catalog

EiaSearchParams = CatalogSearchParams

PARSIMONY_EIA_CATALOG_URL_ENV = "PARSIMONY_EIA_CATALOG_URL"

EIA_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

eia_search = make_local_search_connector(
    provider="eia",
    default_url="hf://parsimony-dev/eia",
    catalog_url_env_var=PARSIMONY_EIA_CATALOG_URL_ENV,
    build_catalog=build_eia_catalog,
    tags=["macro", "energy", "us", "tool"],
    description=(
        "Semantic-search the U.S. Energy Information Administration (EIA) Open Data catalog of "
        "energy datasets (petroleum, natural gas, electricity, coal, nuclear, renewables, "
        "emissions, international). Each hit is a dataset route plus its measure and facet "
        "vocabulary; pass the route to eia_fetch(route=..., measure=..., facets=...) for "
        "observations, eia_facets(route=..., facet=...) to list a facet's values, or use "
        "eia_fetch_series(series_id=...) for a known legacy series id like PET.RWTC.D."
    ),
    output_columns=EIA_SEARCH_OUTPUT.columns,
)

__all__ = ["PARSIMONY_EIA_CATALOG_URL_ENV", "EiaSearchParams", "eia_search"]
