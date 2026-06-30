"""World Bank connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_worldbank.connectors.fetch import worldbank_fetch
from parsimony_worldbank.connectors.search import worldbank_search

CONNECTORS = Connectors([worldbank_fetch, worldbank_search])

__all__ = ["CONNECTORS"]
