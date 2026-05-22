"""Semantic search over the published Banco de Portugal (BdP) catalog."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.utils.catalog_search import CatalogSearchParams, make_catalog_search_connector

BdpSearchParams = CatalogSearchParams

PARSIMONY_BDP_CATALOG_URL_ENV = "PARSIMONY_BDP_CATALOG_URL"

BDP_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

bdp_search = make_catalog_search_connector(
    provider="bdp",
    default_url="hf://parsimony-dev/bdp",
    env_var=PARSIMONY_BDP_CATALOG_URL_ENV,
    tags=["macro", "pt", "tool"],
    description=(
        "Semantic-search the Banco de Portugal (BdP) BPstat catalog. "
        "Pass series codes to bdp_fetch after splitting domain_id:dataset_id:series_id."
    ),
    output_columns=BDP_SEARCH_OUTPUT.columns,
)
