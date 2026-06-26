"""bde connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.connectors.fetch import bde_fetch
from parsimony_bde.search import bde_search

CONNECTORS = Connectors([bde_fetch, enumerate_bde, bde_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog URL bound on search."""
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = ["CONNECTORS", "load"]
