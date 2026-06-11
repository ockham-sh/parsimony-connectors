"""Build the Destatis catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_destatis.connectors.enumerate import enumerate_destatis
from parsimony_destatis.outputs import DESTATIS_ENUMERATE_OUTPUT

CATALOG_NAMESPACE = "destatis"


def build_destatis_catalog() -> Catalog:
    result = enumerate_destatis()
    entries = entities_from_raw(result, DESTATIS_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_destatis_catalog"]
