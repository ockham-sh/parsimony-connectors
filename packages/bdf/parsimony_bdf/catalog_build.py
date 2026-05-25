"""Build the Banque de France catalog snapshot."""

from __future__ import annotations

import os

from parsimony.catalog import Catalog
from parsimony.catalog.source import entities_from_raw
from parsimony.catalog.policy import discovery_indexes
from parsimony.errors import ConnectorError

from parsimony_bdf import enumerate_bdf

CATALOG_NAMESPACE = "bdf"
_BDF_API_KEY_ENV = "BDF_API_KEY"


async def build_bdf_catalog(*, api_key: str | None = None) -> Catalog:
    key = (api_key or os.environ.get(_BDF_API_KEY_ENV, "")).strip()
    if not key:
        raise ConnectorError(
            f"BdF catalog build requires {_BDF_API_KEY_ENV} or api_key.",
            provider="bdf",
        )
    result = await enumerate_bdf(api_key=key)
    entries = entities_from_raw(result, result.output_schema)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bdf_catalog"]
