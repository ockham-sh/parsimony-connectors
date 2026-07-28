"""Semantic search over the published Banco de Portugal (BdP) catalog."""

from __future__ import annotations

from parsimony.catalog.search import (
    CatalogSearchParams,
    make_local_search_connector,
)
from parsimony.result import Column, ColumnRole, OutputSpec

BdpSearchParams = CatalogSearchParams

PARSIMONY_BDP_CATALOG_URL_ENV = "PARSIMONY_BDP_CATALOG_URL"

BDP_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="domain_id", role=ColumnRole.METADATA),
        Column(name="domain_name", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(name="dataset_label", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),
        Column(name="short_label", role=ColumnRole.METADATA),
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
        "macro/monetary/financial time series. query= is literal text; use "
        "filter={'code': '...'} for an exact id. Returns ranked codes. A series "
        "code splits as domain_id:dataset_id:series_id — pass those pieces to "
        "bdp_fetch (series_id via the series_ids= kwarg). Codes prefixed "
        "domain:/dataset: are navigation stubs, not directly fetchable."
    ),
    output=BDP_SEARCH_OUTPUT,
)
