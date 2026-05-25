"""Build the Bank of Canada catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.source import entities_from_raw
from parsimony.catalog.policy import discovery_indexes

from parsimony_boc import enumerate_boc

CATALOG_NAMESPACE = "boc"


async def build_boc_catalog() -> Catalog:
    result = await enumerate_boc()
    entries = entities_from_raw(result, result.output_schema)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_boc_catalog"]
