"""bde connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.connectors.fetch import bde_fetch
from parsimony_bde.search import bde_search

CONNECTORS = Connectors([bde_fetch, enumerate_bde, bde_search])

__all__ = ["CONNECTORS"]
