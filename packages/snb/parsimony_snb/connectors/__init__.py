"""The SNB connector bundle (the discovered plugin surface)."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_snb.connectors.enumerate import enumerate_snb
from parsimony_snb.connectors.fetch import snb_fetch
from parsimony_snb.search import snb_search

CONNECTORS = Connectors([snb_fetch, enumerate_snb, snb_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog-search URL bound.

    SNB is keyless, so there is no API key to bind — only the catalog snapshot URL
    for ``snb_search`` (overrides the published default / ``PARSIMONY_SNB_CATALOG_URL``
    env var when supplied).
    """
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = ["CONNECTORS", "load", "enumerate_snb", "snb_fetch", "snb_search"]
