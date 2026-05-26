"""Semantic search over the published Banco de Portugal (BdP) catalog."""

from __future__ import annotations

from parsimony.catalog.search import CatalogSearchParams, make_local_search_connector
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_bdp.catalog_build import build_bdp_catalog

BdpSearchParams = CatalogSearchParams

PARSIMONY_BDP_CATALOG_URL_ENV = "PARSIMONY_BDP_CATALOG_URL"

BDP_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

bdp_search = make_local_search_connector(
    provider="bdp",
    default_url="hf://parsimony-dev/bdp",
    catalog_url_env_var=PARSIMONY_BDP_CATALOG_URL_ENV,
    build_catalog=build_bdp_catalog,
    tags=["macro", "pt", "tool"],
    description=(
        "Semantic-search the Banco de Portugal (BdP) BPstat catalog. "
        "Pass series codes to bdp_fetch after splitting domain_id:dataset_id:series_id."
    ),
    output_columns=BDP_SEARCH_OUTPUT.columns,
)
