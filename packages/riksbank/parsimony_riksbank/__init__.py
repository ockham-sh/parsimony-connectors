"""Sveriges Riksbank connector plugin — all five public Riksbank REST products.

The Riksbank publishes five keyless JSON APIs behind one Azure APIM gateway
(``api.riksbank.se``); this plugin surfaces every one:

1. **SWEA** (``swea/v1``) — interest rates & exchange rates (~117 series). Fetch by id
   via :func:`riksbank_fetch`.
2. **SWESTR** (``swestr/v1``) — the Swedish Krona Short-Term Rate, its five compounded
   averages and its index (7 series). Fetch via :func:`riksbank_swestr_fetch`.
3. **Monetary Policy Data** (``monetary_policy_data/v1``) — the forecasts & outcomes
   behind each Monetary Policy Report (~24 series across ~59 policy-round vintages).
   Fetch via :func:`riksbank_monetary_policy_fetch`.
4. **Turnover Statistics** (``turnover-statistics/v1``) — aggregated turnover on the
   Swedish fixed-income, FX and interest-rate-derivative markets (6 market x frequency
   datasets, history since 1987). Fetch via :func:`riksbank_turnover_fetch`.
5. **Holdings** (``holdings/v1``) — the Riksbank's holdings of Swedish securities
   (per-security and aggregated). Fetch via :func:`riksbank_holdings_fetch`.

All five are **open / keyless**: the ``Ocp-Apim-Subscription-Key`` header is optional and
only raises the keyless quota (5 requests/minute, 1000/day per IP). Each catalog row
carries a ``source`` so an agent routes a search hit to the right fetch verb, and the
``code`` shape encodes the route (SWEA/SWESTR bare ids; the others ``<family>/...``).

This module is the thin public facade: discovery loads ``CONNECTORS`` from here, and the
implementation lives in focused submodules (``_http``, ``swea``, ``swestr``,
``monetary_policy``, ``turnover``, ``holdings``, ``outputs``, ``connectors/*``, ``search``,
``catalog_build``).
"""

from __future__ import annotations

from parsimony_riksbank.connectors import (
    CONNECTORS,
    enumerate_riksbank,
    load,
    riksbank_fetch,
    riksbank_holdings_fetch,
    riksbank_monetary_policy_fetch,
    riksbank_swestr_fetch,
    riksbank_turnover_fetch,
)
from parsimony_riksbank.outputs import (
    RIKSBANK_ENUMERATE_OUTPUT,
    RIKSBANK_FETCH_OUTPUT,
    RIKSBANK_HOLDINGS_FETCH_OUTPUT,
    RIKSBANK_MONETARY_POLICY_FETCH_OUTPUT,
    RIKSBANK_SWESTR_FETCH_OUTPUT,
    RIKSBANK_TURNOVER_FETCH_OUTPUT,
)
from parsimony_riksbank.search import (
    PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    RIKSBANK_SEARCH_OUTPUT,
    RiksbankSearchParams,
    riksbank_search,
)
from parsimony_riksbank.swestr import SwestrSeries

# Convenience re-exports for callers and tests (the canonical homes are the submodules).
_ = (
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_swestr_fetch,
    riksbank_monetary_policy_fetch,
    riksbank_turnover_fetch,
    riksbank_holdings_fetch,
    riksbank_search,
    RIKSBANK_ENUMERATE_OUTPUT,
    RIKSBANK_FETCH_OUTPUT,
    RIKSBANK_SWESTR_FETCH_OUTPUT,
    RIKSBANK_MONETARY_POLICY_FETCH_OUTPUT,
    RIKSBANK_TURNOVER_FETCH_OUTPUT,
    RIKSBANK_HOLDINGS_FETCH_OUTPUT,
    RIKSBANK_SEARCH_OUTPUT,
    RiksbankSearchParams,
    PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    SwestrSeries,
)

__all__ = ["CONNECTORS", "load"]
