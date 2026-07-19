"""Build the Bank of Canada catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_boc.connectors.enumerate import enumerate_boc

CATALOG_NAMESPACE = "boc"


def build_boc_catalog() -> Catalog:
    result = enumerate_boc()
    entries = list(result.entities.values())
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_boc_catalog"]
