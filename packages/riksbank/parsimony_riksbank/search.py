"""Semantic search over the published Sveriges Riksbank catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

RiksbankSearchParams = CatalogSearchParams

PARSIMONY_RIKSBANK_CATALOG_URL_ENV = "PARSIMONY_RIKSBANK_CATALOG_URL"

RIKSBANK_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

riksbank_search = make_catalog_search_connector(
    provider="riksbank",
    default_url="hf://parsimony-dev/riksbank",
    env_var=PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    tags=["macro", "se", "tool"],
    description=(
        "Semantic-search the Sveriges Riksbank catalog. "
        "SWESTR identifiers → riksbank_swestr_fetch(series=...); "
        "SWEA series → riksbank_fetch(series_id=...)."
    ),
    output_columns=RIKSBANK_SEARCH_OUTPUT.columns,
)
