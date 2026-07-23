"""Credential-declaration contract for parsimony-eodhd.

One :class:`CredentialDeclarationSuite` subclass per HTTP-calling verb. Each
proves the connector's ``requires=("EODHD_API_KEY",)`` declaration matches
runtime: the bare call fast-fails naming that env var without touching the
network, and an env-supplied key reaches the outgoing request (EODHD carries it
as the ``api_token`` query parameter).
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_eodhd import (
    eodhd_bulk_eod,
    eodhd_calendar,
    eodhd_dividends,
    eodhd_eod,
    eodhd_exchange_symbols,
    eodhd_exchanges,
    eodhd_fundamentals,
    eodhd_insider,
    eodhd_intraday,
    eodhd_live,
    eodhd_macro,
    eodhd_macro_bulk,
    eodhd_news,
    eodhd_screener,
    eodhd_search,
    eodhd_splits,
    eodhd_technical,
)

_BASE = "https://eodhd.com/api"


class TestEodhdSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_search
    call_kwargs = {"query": "apple"}
    route_url = f"{_BASE}/search/apple"


class TestEodhdExchangesCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_exchanges
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/exchanges-list"


class TestEodhdExchangeSymbolsCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_exchange_symbols
    call_kwargs = {"exchange": "US"}
    route_url = f"{_BASE}/exchange-symbol-list/US"


class TestEodhdEodCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_eod
    call_kwargs = {"ticker": "AAPL.US"}
    route_url = f"{_BASE}/eod/AAPL.US"


class TestEodhdLiveCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_live
    call_kwargs = {"ticker": "AAPL.US"}
    route_url = f"{_BASE}/real-time/AAPL.US"


class TestEodhdIntradayCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_intraday
    call_kwargs = {"ticker": "AAPL.US", "interval": "5m"}
    route_url = f"{_BASE}/intraday/AAPL.US"


class TestEodhdBulkEodCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_bulk_eod
    call_kwargs = {"exchange": "US"}
    route_url = f"{_BASE}/eod-bulk-last-day/US"


class TestEodhdDividendsCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_dividends
    call_kwargs = {"ticker": "AAPL.US"}
    route_url = f"{_BASE}/div/AAPL.US"


class TestEodhdSplitsCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_splits
    call_kwargs = {"ticker": "AAPL.US"}
    route_url = f"{_BASE}/splits/AAPL.US"


class TestEodhdFundamentalsCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_fundamentals
    call_kwargs = {"ticker": "AAPL.US"}
    route_url = f"{_BASE}/fundamentals/AAPL.US"


class TestEodhdCalendarCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_calendar
    call_kwargs = {"type": "earnings"}
    route_url = f"{_BASE}/calendar/earnings"


class TestEodhdNewsCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_news
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/news"


class TestEodhdMacroCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_macro
    call_kwargs = {"country": "USA", "indicator": "gdp_current_usd"}
    route_url = f"{_BASE}/macro-indicator/USA"


class TestEodhdMacroBulkCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_macro_bulk
    call_kwargs = {"country": "USA"}
    route_url = f"{_BASE}/macro-indicator/USA"


class TestEodhdTechnicalCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_technical
    call_kwargs = {"ticker": "AAPL.US", "function": "sma"}
    route_url = f"{_BASE}/technical/AAPL.US"


class TestEodhdInsiderCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_insider
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/insider-transactions"


class TestEodhdScreenerCredentialDeclaration(CredentialDeclarationSuite):
    connector = eodhd_screener
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/screener"
