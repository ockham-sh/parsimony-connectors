"""Build the Banque de France catalog snapshot.

Maintainer tooling, not part of the plugin contract: ``enumerate_bdf`` streams
the full ``series`` universe, the rows become catalog entities, and the catalog
is indexed and built. Titles are already bilingual at the source (English short
title with a French / breadcrumb fallback in ``description``), so — unlike BdE —
no separate enrichment pass is needed.
"""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_bdf.connectors.enumerate import enumerate_bdf

CATALOG_NAMESPACE = "bdf"


def build_bdf_catalog(*, api_key: str | None = None) -> Catalog:
    """Enumerate the full BdF universe and build a searchable catalog snapshot.

    ``api_key`` falls back to ``BDF_API_KEY`` inside ``enumerate_bdf`` (and
    fast-fails with :class:`~parsimony.errors.UnauthorizedError` if neither is
    set), so the snapshot can be built straight from the environment.
    """
    result = enumerate_bdf(api_key=(api_key or "").strip())
    entries = list(result.entities.values())
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bdf_catalog"]
