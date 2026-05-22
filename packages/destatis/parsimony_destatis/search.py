"""Semantic search over the published Destatis catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

DestatisSearchParams = CatalogSearchParams

PARSIMONY_DESTATIS_CATALOG_URL_ENV = "PARSIMONY_DESTATIS_CATALOG_URL"

DESTATIS_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

destatis_search = make_catalog_search_connector(
    provider="destatis",
    default_url="hf://parsimony-dev/destatis",
    env_var=PARSIMONY_DESTATIS_CATALOG_URL_ENV,
    tags=["macro", "de", "tool"],
    description=(
        "Semantic-search the German Federal Statistical Office (Destatis) GENESIS-Online catalog. "
        "Pass returned code to destatis_fetch(name=...)."
    ),
    output_columns=DESTATIS_SEARCH_OUTPUT.columns,
)
