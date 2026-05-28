"""Build the Reserve Bank of Australia catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_rba import RBA_ENUMERATE_OUTPUT, enumerate_rba

CATALOG_NAMESPACE = "rba"


async def build_rba_catalog() -> Catalog:
    result = await enumerate_rba()
    entries = entities_from_raw(result, RBA_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_rba_catalog"]
