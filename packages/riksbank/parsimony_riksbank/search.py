"""Semantic search over the published Sveriges Riksbank catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector

RiksbankSearchParams = CatalogSearchParams

PARSIMONY_RIKSBANK_CATALOG_URL_ENV = "PARSIMONY_RIKSBANK_CATALOG_URL"

RIKSBANK_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

from parsimony_riksbank.catalog_build import build_riksbank_catalog

riksbank_search = make_local_search_connector(
    provider="riksbank",
    default_url="hf://parsimony-dev/riksbank",
    catalog_url_env_var=PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    build_catalog=build_riksbank_catalog,
    tags=["macro", "se", "tool"],
    description=(
        "Semantic-search the Sveriges Riksbank catalog. "
        "SWESTR identifiers → riksbank_swestr_fetch(series=...); "
        "SWEA series → riksbank_fetch(series_id=...)."
    ),
    output_columns=RIKSBANK_SEARCH_OUTPUT.columns,
)
