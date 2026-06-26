"""destatis connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_destatis.connectors.enumerate import enumerate_destatis
from parsimony_destatis.connectors.fetch import destatis_fetch
from parsimony_destatis.search import destatis_search

CONNECTORS = Connectors([destatis_fetch, enumerate_destatis, destatis_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog URL bound on search."""
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = ["CONNECTORS", "load"]
