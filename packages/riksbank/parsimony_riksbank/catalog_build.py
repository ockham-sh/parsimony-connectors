"""Build the Riksbank catalog snapshot."""

from __future__ import annotations

import os

from parsimony.catalog import Catalog
from parsimony.catalog.source import entities_from_raw
from parsimony.catalog.policy import discovery_indexes

from parsimony_riksbank import enumerate_riksbank

CATALOG_NAMESPACE = "riksbank"
_RIKSBANK_API_KEY_ENV = "RIKSBANK_API_KEY"


async def build_riksbank_catalog(*, api_key: str | None = None) -> Catalog:
    key = api_key if api_key is not None else os.environ.get(_RIKSBANK_API_KEY_ENV, "")
    result = await enumerate_riksbank(api_key=key)
    entries = entities_from_raw(result, result.output_schema)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_riksbank_catalog"]
