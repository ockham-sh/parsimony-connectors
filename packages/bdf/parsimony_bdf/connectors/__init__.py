"""bdf connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bdf.connectors.enumerate import enumerate_bdf
from parsimony_bdf.connectors.fetch import bdf_fetch
from parsimony_bdf.search import bdf_search

CONNECTORS = Connectors([bdf_fetch, enumerate_bdf, bdf_search])


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every keyed connector."""
    return CONNECTORS.bind(api_key=api_key)


__all__ = ["CONNECTORS", "load"]
