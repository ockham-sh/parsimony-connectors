"""US Treasury connector — fetch + catalog enumeration. Keyless public source.

Two transports, one catalog:

* the **Fiscal Data JSON API** (``api.fiscaldata.treasury.gov``) — federal fiscal datasets
  (debt, receipts, spending, securities, auctions, exchange rates, certified interest
  rates); fetched by endpoint path via ``treasury_fetch``; and
* the **Office of Debt Management** daily interest-rate feeds (``home.treasury.gov``) —
  OData/Atom **XML** (par yield curve, real yield curve, bill rates, long-term rates),
  fetched per calendar year via ``treasury_rates_fetch``.

Discovery (no native search → a built catalog, two enumeration sources):

* **Fiscal Data — archetype A.** The live ``/services/dtg/metadata/`` JSON (the same
  source the fiscaldata SPA consumes) lists every dataset → endpoint → field in one call,
  so the catalog self-tracks new datasets. One row per measure field
  (``{endpoint}#{field}``).
* **ODM rate feeds — archetype D.** A curated 5-feed registry (the interest-rate-statistics
  dropdown is the authoritative, stable feed list). One row per benchmark maturity
  (``home/{feed}#{column}``). ``scripts/harvest_rate_feeds.py`` cross-validates the
  registry's columns against the live feeds.

Keyless — no ``secrets=``/``bind()``/``UnauthorizedError``; ``load()`` binds only the
catalog URL for ``treasury_search``.

This module is a thin facade: the implementation lives in :mod:`parsimony_treasury._http`,
:mod:`parsimony_treasury.parsing`, :mod:`parsimony_treasury.rate_feeds`,
:mod:`parsimony_treasury.outputs`, :mod:`parsimony_treasury.connectors`, and
:mod:`parsimony_treasury.search`.
"""

from __future__ import annotations

from parsimony_treasury.connectors import (
    CONNECTORS,
    enumerate_treasury,
    load,
    treasury_fetch,
    treasury_rates_fetch,
)
from parsimony_treasury.outputs import (
    TREASURY_ENUMERATE_OUTPUT,
    TREASURY_FETCH_OUTPUT,
    TREASURY_RATES_FETCH_OUTPUT,
)
from parsimony_treasury.search import (
    PARSIMONY_TREASURY_CATALOG_URL_ENV,
    TREASURY_SEARCH_OUTPUT,
    treasury_search,
)

__all__ = ["CONNECTORS", "load"]

# Re-exported for downstream/test convenience; the discovered plugin surface is CONNECTORS
# and the only public entry points are CONNECTORS + load (hence __all__).
_ = (
    enumerate_treasury,
    treasury_fetch,
    treasury_rates_fetch,
    treasury_search,
    TREASURY_ENUMERATE_OUTPUT,
    TREASURY_FETCH_OUTPUT,
    TREASURY_RATES_FETCH_OUTPUT,
    TREASURY_SEARCH_OUTPUT,
    PARSIMONY_TREASURY_CATALOG_URL_ENV,
)
