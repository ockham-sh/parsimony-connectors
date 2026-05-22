"""Semantic search over the published Bank of Japan (BoJ) catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

BojSearchParams = CatalogSearchParams

PARSIMONY_BOJ_CATALOG_URL_ENV = "PARSIMONY_BOJ_CATALOG_URL"

BOJ_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

boj_search = make_catalog_search_connector(
    provider="boj",
    default_url="hf://parsimony-dev/boj",
    env_var=PARSIMONY_BOJ_CATALOG_URL_ENV,
    tags=["macro", "jp", "tool"],
    description=(
        "Semantic-search the Bank of Japan (BoJ) stat_search catalog. "
        "Pass returned code to boj_fetch(db=..., code=...)."
    ),
    output_columns=BOJ_SEARCH_OUTPUT.columns,
)
