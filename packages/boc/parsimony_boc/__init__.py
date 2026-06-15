"""Bank of Canada (BoC) connector: fetch + catalog enumeration + search.

Valet API (``https://www.bankofcanada.ca/valet``) — keyless public JSON. Three
connectors, discovered as the top-level ``CONNECTORS`` bundle:

* ``boc_fetch`` — observations by series name(s) or ``group:NAME`` panel.
* ``enumerate_boc`` — the catalog feed (archetype A: live ``/lists/series``).
* ``boc_search`` — search over the published catalog snapshot.

This module is a thin facade; the implementation lives in ``_http`` / ``outputs``
/ ``connectors/{fetch,enumerate}`` / ``search`` / ``catalog_build``.
"""

from __future__ import annotations

from parsimony_boc.connectors import CONNECTORS, load
from parsimony_boc.connectors.enumerate import enumerate_boc
from parsimony_boc.connectors.fetch import boc_fetch
from parsimony_boc.outputs import BOC_ENUMERATE_OUTPUT, BOC_FETCH_OUTPUT
from parsimony_boc.search import BOC_SEARCH_OUTPUT, PARSIMONY_BOC_CATALOG_URL_ENV, boc_search

__all__ = [
    "BOC_ENUMERATE_OUTPUT",
    "BOC_FETCH_OUTPUT",
    "BOC_SEARCH_OUTPUT",
    "CONNECTORS",
    "PARSIMONY_BOC_CATALOG_URL_ENV",
    "boc_fetch",
    "boc_search",
    "enumerate_boc",
    "load",
]
