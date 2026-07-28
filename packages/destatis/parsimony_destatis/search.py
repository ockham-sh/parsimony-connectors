"""Semantic search over the published Destatis catalog."""

from __future__ import annotations

from parsimony.catalog.search import (
    CatalogSearchParams,
    make_local_search_connector,
)
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_destatis.catalog_build import build_destatis_catalog

DestatisSearchParams = CatalogSearchParams

PARSIMONY_DESTATIS_CATALOG_URL_ENV = "PARSIMONY_DESTATIS_CATALOG_URL"

# METADATA columns are projected from catalog entity metadata. Declaring them on
# the output spec is the single source of truth for hit shape / ``describe()``.
DESTATIS_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),
        Column(name="subject_area", role=ColumnRole.METADATA),
        Column(name="parent_statistic", role=ColumnRole.METADATA),
    ]
)

destatis_search = make_local_search_connector(
    provider="destatis",
    default_url="hf://parsimony-dev/destatis",
    catalog_url_env_var=PARSIMONY_DESTATIS_CATALOG_URL_ENV,
    build_catalog=build_destatis_catalog,
    tags=["macro", "de", "tool"],
    description=(
        "Search the German Federal Statistical Office (Destatis) GENESIS-Online catalog. "
        "Pass returned code to destatis_fetch(name=...)."
    ),
    output=DESTATIS_SEARCH_OUTPUT,
)
