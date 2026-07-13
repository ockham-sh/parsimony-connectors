"""Build the Reserve Bank of Australia catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_rba.connectors.enumerate import enumerate_rba

CATALOG_NAMESPACE = "rba"


def build_rba_catalog() -> Catalog:
    result = enumerate_rba()
    entries = result.to_entities()
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_rba_catalog"]
