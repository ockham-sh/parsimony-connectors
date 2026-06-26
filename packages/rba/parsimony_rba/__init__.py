"""Reserve Bank of Australia (RBA) connector — fetch + catalog enumeration.

Data: https://www.rba.gov.au/statistics/tables/. No authentication required
(keyless public statistics site — no ``secrets=``/``bind()``/``UnauthorizedError``;
``load()`` binds only the catalog URL for search).

Transport — the Akamai / ``curl_cffi`` special case
---------------------------------------------------
``rba.gov.au`` is fronted by Akamai bot-mitigation that **TLS-fingerprint-blocks
stock python-httpx** (HTTP 403), so the canonical ``make_http_client``/``fetch_json``
path structurally cannot reach this host. RBA requests go through **curl_cffi**
(``impersonate="chrome"``), a HARD dependency, with a hand-written error mapper
(:mod:`parsimony_rba._http`) — the §6 "raw transport + custom mapper" carve-out.

Discovery (the 3-pass HTML scrape, archetype E — see :mod:`parsimony_rba.connectors.enumerate`):

1. **CSV index** (``/statistics/tables/``): ~216 CSVs / ~3,958 active series — the bulk.
2. **Current XLSX-exclusive sheets** (``/statistics/tables/xls/``): one sheet not
   re-exported as CSV (``a03`` → "Bond Purchase Program"), found by dynamic exclusivity.
3. **Legacy xls-hist binaries** (``/statistics/historical-data.html``): ~26 ``.xls``
   workbooks of discontinued series (~200) that left the live CSVs.

``rba_fetch`` resolves a ``table_id`` across all three formats, so every catalogued
series is fetchable.

This module is a thin facade: the implementation lives in :mod:`parsimony_rba._http`,
:mod:`parsimony_rba.parsing`, :mod:`parsimony_rba.outputs`,
:mod:`parsimony_rba.connectors`, and :mod:`parsimony_rba.search`.
"""

from __future__ import annotations

from parsimony_rba.connectors import CONNECTORS, load
from parsimony_rba.connectors.enumerate import enumerate_rba
from parsimony_rba.connectors.fetch import rba_fetch
from parsimony_rba.outputs import RBA_ENUMERATE_OUTPUT, RBA_FETCH_OUTPUT
from parsimony_rba.search import rba_search

__all__ = ["CONNECTORS", "load"]

# Names re-exported above (enumerate_rba, rba_fetch, rba_search, the OUTPUT configs)
# are kept importable from the package root for downstream/test convenience; the
# discovered plugin surface is CONNECTORS, and the only public entry points are
# CONNECTORS + load (hence __all__).
_ = (enumerate_rba, rba_fetch, rba_search, RBA_ENUMERATE_OUTPUT, RBA_FETCH_OUTPUT)
