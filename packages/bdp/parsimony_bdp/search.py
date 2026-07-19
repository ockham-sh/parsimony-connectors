"""Semantic search over the published Banco de Portugal (BdP) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputSpec

BdpSearchParams = CatalogSearchParams

PARSIMONY_BDP_CATALOG_URL_ENV = "PARSIMONY_BDP_CATALOG_URL"

BDP_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

bdp_search = make_local_search_connector(
    provider="bdp",
    default_url="hf://parsimony-dev/bdp",
    catalog_url_env_var=PARSIMONY_BDP_CATALOG_URL_ENV,
    build_catalog=None,  # async build; use scripts/build_catalog.py instead
    tags=["macro", "pt", "tool"],
    description=(
        "Search the Banco de Portugal (BdP) BPstat catalog of Portuguese "
        "macro/monetary/financial time series. Returns ranked codes. A series code "
        "splits as domain_id:dataset_id:series_id — pass those to bdp_fetch "
        "(series_id via the series_ids filter). Codes prefixed domain:/dataset: are "
        "navigation stubs, not directly fetchable."
    ),
    output_columns=BDP_SEARCH_OUTPUT.columns,
)
