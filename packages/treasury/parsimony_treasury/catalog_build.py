"""Build the U.S. Treasury catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_treasury import TREASURY_ENUMERATE_OUTPUT, enumerate_treasury

CATALOG_NAMESPACE = "treasury"


def build_treasury_catalog() -> Catalog:
    result = enumerate_treasury()
    entries = entities_from_raw(result, TREASURY_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_treasury_catalog"]
