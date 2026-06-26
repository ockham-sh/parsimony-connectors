"""boc connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_boc.connectors.enumerate import enumerate_boc
from parsimony_boc.connectors.fetch import boc_fetch
from parsimony_boc.search import boc_search

CONNECTORS = Connectors([boc_fetch, enumerate_boc, boc_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with the optional search catalog URL bound.

    BoC is keyless — there is no API key to bind. ``catalog_url`` lets an
    operator point ``boc_search`` at a specific catalog snapshot.
    """
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = ["CONNECTORS", "load"]
