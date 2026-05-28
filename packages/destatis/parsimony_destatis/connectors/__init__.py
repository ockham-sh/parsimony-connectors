"""destatis connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_destatis.connectors.enumerate import enumerate_destatis
from parsimony_destatis.connectors.fetch import destatis_fetch
from parsimony_destatis.search import destatis_search

CONNECTORS = Connectors([destatis_fetch, enumerate_destatis, destatis_search])

__all__ = ["CONNECTORS"]
