"""Build the U.S. Treasury catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_treasury.connectors.enumerate import enumerate_treasury

CATALOG_NAMESPACE = "treasury"


def build_treasury_catalog() -> Catalog:
    result = enumerate_treasury()
    entries = result.to_entities()
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_treasury_catalog"]
