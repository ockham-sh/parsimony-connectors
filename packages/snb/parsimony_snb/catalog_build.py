"""Build the Swiss National Bank catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_snb import SNB_ENUMERATE_OUTPUT, enumerate_snb

CATALOG_NAMESPACE = "snb"


def build_snb_catalog() -> Catalog:
    result = enumerate_snb()
    entries = entities_from_raw(result, SNB_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_snb_catalog"]
