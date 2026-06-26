"""boj connector registry."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_boj.connectors.enumerate import enumerate_boj
from parsimony_boj.connectors.fetch import boj_fetch
from parsimony_boj.search import boj_databases_search, boj_series_search

CONNECTORS = Connectors([boj_fetch, enumerate_boj, boj_databases_search, boj_series_search])


def load(*, catalog_root: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with the optional catalog root bound.

    BoJ is keyless — there is no API key to bind. ``catalog_root`` lets an
    operator point the two search connectors at a specific multi-bundle snapshot
    (it is scoped to the connectors that expose it).
    """
    if catalog_root is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_root=catalog_root)


__all__ = ["CONNECTORS", "load"]
