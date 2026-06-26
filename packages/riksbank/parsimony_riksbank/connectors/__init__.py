"""The Sveriges Riksbank connector bundle (the discovered plugin surface)."""

from __future__ import annotations

from parsimony.connector import Connectors

# Import the fetch + enumerate verbs BEFORE search: search -> catalog_build ->
# connectors.enumerate, so the enumerate submodule must already be in sys.modules when
# search is imported (else the partial-package import cycle bites).
from parsimony_riksbank.connectors.enumerate import enumerate_riksbank
from parsimony_riksbank.connectors.fetch import (
    riksbank_fetch,
    riksbank_holdings_fetch,
    riksbank_monetary_policy_fetch,
    riksbank_swestr_fetch,
    riksbank_turnover_fetch,
)
from parsimony_riksbank.search import riksbank_search

CONNECTORS = Connectors(
    [
        riksbank_fetch,
        riksbank_swestr_fetch,
        riksbank_monetary_policy_fetch,
        riksbank_turnover_fetch,
        riksbank_holdings_fetch,
        enumerate_riksbank,
        riksbank_search,
    ]
)


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog-search URL bound.

    Riksbank is keyless, so there is no API key to bind — only the catalog snapshot URL
    for ``riksbank_search`` (overrides the published default / ``PARSIMONY_RIKSBANK_CATALOG_URL``
    env var when supplied).
    """
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = [
    "CONNECTORS",
    "load",
    "riksbank_fetch",
    "riksbank_swestr_fetch",
    "riksbank_monetary_policy_fetch",
    "riksbank_turnover_fetch",
    "riksbank_holdings_fetch",
    "enumerate_riksbank",
    "riksbank_search",
]
