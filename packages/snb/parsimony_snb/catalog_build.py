"""Build the Swiss National Bank catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_snb.connectors.enumerate import enumerate_snb

CATALOG_NAMESPACE = "snb"


def build_snb_catalog() -> Catalog:
    result = enumerate_snb()
    entries = list(result.entities.values())
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_snb_catalog"]
