"""Build the Banco de España catalog snapshot."""

from __future__ import annotations

import logging

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_bde.connectors.enumerate import enumerate_bde

logger = logging.getLogger(__name__)

CATALOG_NAMESPACE = "bde"


def build_bde_catalog() -> Catalog:
    """Enumerate and build the Banco de España catalog.

    Titles and descriptions are in Spanish — BdE's published catalog CSV
    chapters have no English variant.
    """
    result = enumerate_bde()
    entries = list(result.entities.values())
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bde_catalog"]
