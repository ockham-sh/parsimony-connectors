"""bdp connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bdp.connectors.enumerate import enumerate_bdp
from parsimony_bdp.connectors.fetch import bdp_fetch
from parsimony_bdp.search import bdp_search

CONNECTORS = Connectors([bdp_fetch, enumerate_bdp, bdp_search])

__all__ = ["CONNECTORS"]
