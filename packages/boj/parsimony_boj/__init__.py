"""Bank of Japan (BoJ): fetch + catalog enumeration + two-step search.

BOJ Time-Series Data Search API (``https://www.stat-search.boj.or.jp/api/v1``) —
keyless public JSON. Four connectors, discovered as the top-level ``CONNECTORS``
bundle:

* ``boj_fetch`` — observations by ``(db, code)``; paginates the API's
  60,000-point ``NEXTPOSITION`` limit so multi-series requests never truncate.
* ``enumerate_boj`` — the catalog feed (archetype C + B: the frozen 50-DB
  registry × a live per-DB ``getMetadata`` fan-out).
* ``boj_databases_search`` / ``boj_series_search`` — the two-step discovery chain
  over the published multi-bundle catalog.

This module is a thin facade; the implementation lives in ``_http`` / ``outputs``
/ ``databases`` / ``connectors/{fetch,enumerate}`` / ``search`` / ``catalog_build``.
The private re-exports (``_BOJ_DATABASES``, ``_resolve_boj_database``,
``fetch_boj_enumeration_rows_for_db``) are kept at the top level for the test
suite and the catalog-build helpers.
"""

from __future__ import annotations

from parsimony_boj.connectors import CONNECTORS, load
from parsimony_boj.connectors.enumerate import enumerate_boj
from parsimony_boj.connectors.enumerate import (
    fetch_boj_enumeration_rows_for_db as fetch_boj_enumeration_rows_for_db,  # re-export for tests/catalog_build
)
from parsimony_boj.connectors.fetch import boj_fetch
from parsimony_boj.databases import (
    _BOJ_DATABASES as _BOJ_DATABASES,  # re-export for the test suite
)
from parsimony_boj.databases import (
    _resolve_boj_database as _resolve_boj_database,  # re-export for the test suite
)
from parsimony_boj.outputs import BOJ_ENUMERATE_OUTPUT, BOJ_FETCH_OUTPUT
from parsimony_boj.search import (
    PARSIMONY_BOJ_CATALOG_URL_ENV,
    boj_databases_search,
    boj_series_search,
)

__all__ = [
    "BOJ_ENUMERATE_OUTPUT",
    "BOJ_FETCH_OUTPUT",
    "CONNECTORS",
    "PARSIMONY_BOJ_CATALOG_URL_ENV",
    "boj_databases_search",
    "boj_fetch",
    "boj_series_search",
    "enumerate_boj",
    "load",
]
