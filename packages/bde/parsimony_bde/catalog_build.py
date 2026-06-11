"""Build the Banco de España catalog snapshot."""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.outputs import BDE_ENUMERATE_OUTPUT

CATALOG_NAMESPACE = "bde"


def build_bde_catalog() -> Catalog:
    result = enumerate_bde()
    # The published CSV chapters can list the same series key more than once
    # (e.g. cross-listed aliases). Catalog entities are keyed by ``key`` only.
    df = result.data.drop_duplicates(subset=["key"], keep="first")
    entries = entities_from_raw(df, BDE_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bde_catalog"]
