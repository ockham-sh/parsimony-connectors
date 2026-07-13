"""Build the EIA dataset catalog snapshot.

Maintainer tooling, not part of the plugin contract: ``enumerate_eia`` walks the
v2 route tree to one row per leaf dataset (with its measure/facet manifest), the
rows become catalog entities, and the catalog is indexed and built. The series
within a dataset are not catalogued (EIA's ~2M-series universe is the facet
cartesian product) — they stay fetchable by route+facets or legacy series id.
"""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes

from parsimony_eia.connectors.enumerate import enumerate_eia

CATALOG_NAMESPACE = "eia"


def build_eia_catalog(*, api_key: str | None = None) -> Catalog:
    """Enumerate every EIA leaf dataset and build a searchable catalog snapshot.

    ``api_key`` falls back to ``EIA_API_KEY`` inside ``enumerate_eia`` (which
    fast-fails with :class:`~parsimony.errors.UnauthorizedError` if neither is
    set), so the snapshot can be built straight from the environment.
    """
    result = enumerate_eia(api_key=(api_key or "").strip())
    entries = result.to_entities()
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_eia_catalog"]
