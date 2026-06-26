"""The US Treasury connector bundle (the discovered plugin surface)."""

from __future__ import annotations

from parsimony.connector import Connectors

# Import the fetch + enumerate verbs BEFORE search: search → catalog_build →
# connectors.enumerate, so the enumerate submodule must already be in sys.modules when
# search is imported (else the partial-package import cycle bites).
from parsimony_treasury.connectors.enumerate import enumerate_treasury
from parsimony_treasury.connectors.fetch import treasury_fetch, treasury_rates_fetch
from parsimony_treasury.search import treasury_search

CONNECTORS = Connectors([treasury_fetch, treasury_rates_fetch, enumerate_treasury, treasury_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog-search URL bound.

    Treasury is keyless, so there is no API key to bind — only the catalog snapshot URL
    for ``treasury_search`` (overrides the published default / ``PARSIMONY_TREASURY_CATALOG_URL``
    env var when supplied).
    """
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = [
    "CONNECTORS",
    "load",
    "treasury_fetch",
    "treasury_rates_fetch",
    "enumerate_treasury",
    "treasury_search",
]
