"""Build the Banco de Portugal catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_bdp import BDP_ENUMERATE_OUTPUT, enumerate_bdp

CATALOG_NAMESPACE = "bdp"


async def build_bdp_catalog() -> Catalog:
    result = await enumerate_bdp()
    entries = entities_from_raw(result, BDP_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bdp_catalog"]
