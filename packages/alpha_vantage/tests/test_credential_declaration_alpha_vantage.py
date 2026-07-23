"""Credential-declaration contract for parsimony-alpha-vantage.

One :class:`CredentialDeclarationSuite` subclass per HTTP-calling verb (28
``@connector`` + 1 ``@enumerator``). Each proves the connector's
``requires=("ALPHA_VANTAGE_API_KEY",)`` declaration matches runtime: the bare
call fast-fails naming that env var without touching the network, and an
env-supplied key reaches the outgoing request (Alpha Vantage carries it as the
``apikey`` query parameter). Every JSON and CSV endpoint is the same ``/query``
URL, differentiated by the ``function`` query param, so all verbs share one
``route_url``.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_alpha_vantage.connectors.connectors import (
    alpha_vantage_balance_sheet,
    alpha_vantage_cash_flow,
    alpha_vantage_crypto_daily,
    alpha_vantage_crypto_monthly,
    alpha_vantage_crypto_weekly,
    alpha_vantage_daily,
    alpha_vantage_earnings,
    alpha_vantage_earnings_calendar,
    alpha_vantage_econ,
    alpha_vantage_etf_profile,
    alpha_vantage_fx_daily,
    alpha_vantage_fx_monthly,
    alpha_vantage_fx_rate,
    alpha_vantage_fx_weekly,
    alpha_vantage_income_statement,
    alpha_vantage_intraday,
    alpha_vantage_ipo_calendar,
    alpha_vantage_metal_history,
    alpha_vantage_metal_spot,
    alpha_vantage_monthly,
    alpha_vantage_news,
    alpha_vantage_options,
    alpha_vantage_overview,
    alpha_vantage_quote,
    alpha_vantage_search,
    alpha_vantage_technical,
    alpha_vantage_top_movers,
    alpha_vantage_weekly,
    enumerate_alpha_vantage,
)

_QUERY_URL = "https://www.alphavantage.co/query"


class TestAlphaVantageSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_search
    call_kwargs = {"query": "apple"}
    route_url = _QUERY_URL


class TestAlphaVantageQuoteCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_quote
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageDailyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_daily
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageWeeklyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_weekly
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageMonthlyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_monthly
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageIntradayCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_intraday
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageOverviewCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_overview
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageIncomeStatementCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_income_statement
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageBalanceSheetCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_balance_sheet
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageCashFlowCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_cash_flow
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageEarningsCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_earnings
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageEtfProfileCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_etf_profile
    call_kwargs = {"symbol": "SPY"}
    route_url = _QUERY_URL


class TestAlphaVantageFxRateCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_fx_rate
    call_kwargs = {"from_currency": "EUR", "to_currency": "USD"}
    route_url = _QUERY_URL


class TestAlphaVantageFxDailyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_fx_daily
    call_kwargs = {"from_symbol": "EUR", "to_symbol": "USD"}
    route_url = _QUERY_URL


class TestAlphaVantageFxWeeklyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_fx_weekly
    call_kwargs = {"from_symbol": "EUR", "to_symbol": "USD"}
    route_url = _QUERY_URL


class TestAlphaVantageFxMonthlyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_fx_monthly
    call_kwargs = {"from_symbol": "EUR", "to_symbol": "USD"}
    route_url = _QUERY_URL


class TestAlphaVantageCryptoDailyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_crypto_daily
    call_kwargs = {"symbol": "BTC"}
    route_url = _QUERY_URL


class TestAlphaVantageCryptoWeeklyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_crypto_weekly
    call_kwargs = {"symbol": "BTC"}
    route_url = _QUERY_URL


class TestAlphaVantageCryptoMonthlyCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_crypto_monthly
    call_kwargs = {"symbol": "BTC"}
    route_url = _QUERY_URL


class TestAlphaVantageEconCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_econ
    call_kwargs = {"function": "REAL_GDP"}
    route_url = _QUERY_URL


class TestAlphaVantageMetalSpotCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_metal_spot
    call_kwargs = {"symbol": "GOLD"}
    route_url = _QUERY_URL


class TestAlphaVantageMetalHistoryCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_metal_history
    call_kwargs = {"symbol": "GOLD"}
    route_url = _QUERY_URL


class TestAlphaVantageNewsCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_news
    call_kwargs: dict[str, object] = {}
    route_url = _QUERY_URL


class TestAlphaVantageTopMoversCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_top_movers
    call_kwargs: dict[str, object] = {}
    route_url = _QUERY_URL


class TestAlphaVantageOptionsCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_options
    call_kwargs = {"symbol": "AAPL"}
    route_url = _QUERY_URL


class TestAlphaVantageEarningsCalendarCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_earnings_calendar
    call_kwargs: dict[str, object] = {}
    route_url = _QUERY_URL


class TestAlphaVantageIpoCalendarCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_ipo_calendar
    call_kwargs: dict[str, object] = {}
    route_url = _QUERY_URL


class TestAlphaVantageTechnicalCredentialDeclaration(CredentialDeclarationSuite):
    connector = alpha_vantage_technical
    call_kwargs = {"symbol": "AAPL", "function": "SMA"}
    route_url = _QUERY_URL


class TestAlphaVantageEnumerateCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_alpha_vantage
    call_kwargs: dict[str, object] = {}
    route_url = _QUERY_URL
