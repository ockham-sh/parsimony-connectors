"""The RBA connector bundle (the discovered plugin surface)."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_rba.connectors.enumerate import enumerate_rba
from parsimony_rba.connectors.fetch import rba_fetch
from parsimony_rba.search import rba_search

CONNECTORS = Connectors([rba_fetch, enumerate_rba, rba_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog-search URL bound.

    RBA is keyless, so there is no API key to bind — only the catalog snapshot URL
    for ``rba_search`` (overrides the published default / ``PARSIMONY_RBA_CATALOG_URL``
    env var when supplied).
    """
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = ["CONNECTORS", "load", "enumerate_rba", "rba_fetch", "rba_search"]
