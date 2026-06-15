"""eia connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_eia.connectors.enumerate import enumerate_eia
from parsimony_eia.connectors.fetch import eia_facets, eia_fetch, eia_fetch_series
from parsimony_eia.search import eia_search

CONNECTORS = Connectors([eia_fetch, eia_fetch_series, eia_facets, enumerate_eia, eia_search])


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every keyed connector."""
    return CONNECTORS.bind(api_key=api_key)


__all__ = ["CONNECTORS", "load"]
