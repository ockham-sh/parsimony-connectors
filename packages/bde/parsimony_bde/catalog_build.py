"""Build the Banco de España catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.source import entities_from_raw
from parsimony.catalog.policy import discovery_indexes

from parsimony_bde.connectors.enumerate import enumerate_bde

CATALOG_NAMESPACE = "bde"


async def build_bde_catalog() -> Catalog:
    result = await enumerate_bde()
    entries = entities_from_raw(result, result.output_schema)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bde_catalog"]
