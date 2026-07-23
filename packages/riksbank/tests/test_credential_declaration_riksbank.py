"""Credential-declaration conformance for parsimony-riksbank.

All five Riksbank products are open / keyless: the ``Ocp-Apim-Subscription-Key``
only raises the quota, so no verb declares ``requires=`` and none fast-fails on a
missing key. The applicable suite checks are "undeclared does not fast-fail" and
"bound secret-param canary reaches the request"; the suite self-skips the two
``requires=``-dependent checks.

Verbs that issue more than one request (``riksbank_fetch`` → Observations then a
secondary /Series title lookup; ``enumerate_riksbank`` → /Groups, /Series,
/forecasts/series_ids) mock only the first endpoint; the suite tolerates the
later unmatched call because it only asserts the mocked route was reached and
carried the canary. ``riksbank_search`` is catalog-backed (no HTTP).
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_riksbank import (
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_holdings_fetch,
    riksbank_monetary_policy_fetch,
    riksbank_swestr_fetch,
    riksbank_turnover_fetch,
)


class TestRiksbankFetchCredentials(CredentialDeclarationSuite):
    connector = riksbank_fetch
    call_kwargs = {"series_id": "SEKEURPMI"}
    route_url = "https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI"
    method = "GET"


class TestRiksbankSwestrFetchCredentials(CredentialDeclarationSuite):
    connector = riksbank_swestr_fetch
    call_kwargs = {"series": "SWESTR"}
    route_url = "https://api.riksbank.se/swestr/v1/latest/SWESTR"
    method = "GET"


class TestRiksbankMonetaryPolicyFetchCredentials(CredentialDeclarationSuite):
    connector = riksbank_monetary_policy_fetch
    call_kwargs = {"series": "SEQGDPNAYSA"}
    # The colon-safe raw-httpx path appends a query string; the suite matches any.
    route_url = "https://api.riksbank.se/monetary_policy_data/v1/forecasts"
    method = "GET"


class TestRiksbankTurnoverFetchCredentials(CredentialDeclarationSuite):
    connector = riksbank_turnover_fetch
    call_kwargs = {"market": "fx"}
    route_url = "https://api.riksbank.se/turnover-statistics/v1/markets/fx/frequencies/monthly"
    method = "GET"


class TestRiksbankHoldingsFetchCredentials(CredentialDeclarationSuite):
    connector = riksbank_holdings_fetch
    call_kwargs = {"dataset": "swedish_securities_aggregated"}
    route_url = "https://api.riksbank.se/holdings/v1/swedish_securities_aggregated"
    method = "GET"


class TestEnumerateRiksbankCredentials(CredentialDeclarationSuite):
    connector = enumerate_riksbank
    call_kwargs: dict[str, object] = {}
    route_url = "https://api.riksbank.se/swea/v1/Groups"
    method = "GET"
