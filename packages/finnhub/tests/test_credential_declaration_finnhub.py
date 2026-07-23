"""Credential-declaration conformance for parsimony-finnhub.

One :class:`CredentialDeclarationSuite` subclass per HTTP-calling verb. Each
proves the verb's ``requires=("FINNHUB_API_KEY",)`` declaration matches runtime:
the bare call fast-fails naming that env var, and an env- or bind-supplied key
reaches the outgoing request (finnhub carries it as the ``token`` query param).

Routes are the finnhub base (``https://finnhub.io/api/v1``) plus each verb's
endpoint path; the suite matches any query string, so the ``token`` param does
not need to appear in ``route_url``.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_finnhub import (
    enumerate_finnhub,
    finnhub_basic_financials,
    finnhub_company_news,
    finnhub_earnings,
    finnhub_earnings_calendar,
    finnhub_ipo_calendar,
    finnhub_market_news,
    finnhub_peers,
    finnhub_profile,
    finnhub_quote,
    finnhub_recommendation,
    finnhub_search,
)

_BASE = "https://finnhub.io/api/v1"


class TestFinnhubSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_search
    call_kwargs = {"query": "apple"}
    route_url = f"{_BASE}/search"


class TestFinnhubQuoteCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_quote
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/quote"


class TestFinnhubProfileCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_profile
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/stock/profile2"


class TestFinnhubPeersCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_peers
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/stock/peers"


class TestFinnhubRecommendationCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_recommendation
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/stock/recommendation"


class TestFinnhubEarningsCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_earnings
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/stock/earnings"


class TestFinnhubBasicFinancialsCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_basic_financials
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/stock/metric"


class TestFinnhubCompanyNewsCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_company_news
    call_kwargs = {"symbol": "AAPL", "from_date": "2024-01-01", "to_date": "2024-01-31"}
    route_url = f"{_BASE}/company-news"


class TestFinnhubMarketNewsCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_market_news
    call_kwargs: dict[str, str] = {}
    route_url = f"{_BASE}/news"


class TestFinnhubEarningsCalendarCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_earnings_calendar
    call_kwargs = {"from_date": "2024-01-01", "to_date": "2024-01-31"}
    route_url = f"{_BASE}/calendar/earnings"


class TestFinnhubIpoCalendarCredentialDeclaration(CredentialDeclarationSuite):
    connector = finnhub_ipo_calendar
    call_kwargs = {"from_date": "2024-01-01", "to_date": "2024-01-31"}
    route_url = f"{_BASE}/calendar/ipo"


class TestEnumerateFinnhubCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_finnhub
    call_kwargs: dict[str, str] = {}
    route_url = f"{_BASE}/stock/symbol"
