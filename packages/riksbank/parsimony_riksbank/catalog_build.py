"""Build the Riksbank catalog snapshot."""

from __future__ import annotations

import os

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_riksbank.connectors.enumerate import enumerate_riksbank

CATALOG_NAMESPACE = "riksbank"
_RIKSBANK_API_KEY_ENV = "RIKSBANK_API_KEY"


def build_riksbank_catalog(*, api_key: str | None = None) -> Catalog:
    key = api_key if api_key is not None else os.environ.get(_RIKSBANK_API_KEY_ENV, "")
    result = enumerate_riksbank(api_key=key)
    entries = result.to_entities()
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_riksbank_catalog"]
