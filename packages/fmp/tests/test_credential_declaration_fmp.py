"""Credential-declaration conformance for parsimony-fmp.

One :class:`CredentialDeclarationSuite` subclass per HTTP-calling verb. Each
proves the verb's ``requires=("FMP_API_KEY",)`` declaration matches runtime: the
bare call fast-fails naming that env var, and an env- or bind-supplied key
reaches the outgoing request (FMP carries it as the ``apikey`` query param).

Routes are the FMP base (``https://financialmodelingprep.com/stable``) plus each
verb's endpoint path (for dispatch verbs, the path the default argument selects);
the suite matches any query string, so the ``apikey`` param does not need to
appear in ``route_url``. call_kwargs use free-plan-shaped inputs — the mocked 200
response means plan-gating never triggers, and the suite tolerates any
post-request exception anyway.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_fmp import (
    fmp_analyst_estimates,
    fmp_balance_sheet_statements,
    fmp_cash_flow_statements,
    fmp_company_profile,
    fmp_corporate_history,
    fmp_earnings_transcript,
    fmp_event_calendar,
    fmp_income_statements,
    fmp_index_constituents,
    fmp_insider_trades,
    fmp_institutional_positions,
    fmp_market_movers,
    fmp_news,
    fmp_peers,
    fmp_prices,
    fmp_quotes,
    fmp_screener,
    fmp_search,
    fmp_taxonomy,
)

_BASE = "https://financialmodelingprep.com/stable"


class TestFmpSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_search
    call_kwargs = {"query": "apple"}
    route_url = f"{_BASE}/search-name"


class TestFmpTaxonomyCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_taxonomy
    call_kwargs = {"type": "sectors"}
    route_url = f"{_BASE}/available-sectors"


class TestFmpQuotesCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_quotes
    call_kwargs = {"symbols": "AAPL"}
    route_url = f"{_BASE}/batch-quote"


class TestFmpPricesCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_prices
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/historical-price-eod/full"


class TestFmpCompanyProfileCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_company_profile
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/profile"


class TestFmpPeersCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_peers
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/stock-peers"


class TestFmpIncomeStatementsCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_income_statements
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/income-statement"


class TestFmpBalanceSheetStatementsCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_balance_sheet_statements
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/balance-sheet-statement"


class TestFmpCashFlowStatementsCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_cash_flow_statements
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/cash-flow-statement"


class TestFmpCorporateHistoryCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_corporate_history
    call_kwargs = {"symbol": "AAPL", "event_type": "earnings"}
    route_url = f"{_BASE}/earnings"


class TestFmpEventCalendarCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_event_calendar
    call_kwargs = {"event_type": "earnings"}
    route_url = f"{_BASE}/earnings-calendar"


class TestFmpAnalystEstimatesCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_analyst_estimates
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/analyst-estimates"


class TestFmpNewsCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_news
    call_kwargs = {"type": "news", "symbols": "AAPL"}
    route_url = f"{_BASE}/news/stock"


class TestFmpInsiderTradesCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_insider_trades
    call_kwargs = {"symbol": "AAPL"}
    route_url = f"{_BASE}/insider-trading/search"


class TestFmpInstitutionalPositionsCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_institutional_positions
    call_kwargs = {"symbol": "AAPL", "year": "2023", "quarter": "1"}
    route_url = f"{_BASE}/institutional-ownership/symbol-positions-summary"


class TestFmpEarningsTranscriptCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_earnings_transcript
    call_kwargs = {"symbol": "AAPL", "year": "2023", "quarter": "1"}
    route_url = f"{_BASE}/earning-call-transcript"


class TestFmpIndexConstituentsCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_index_constituents
    call_kwargs = {"index": "SP500"}
    route_url = f"{_BASE}/sp500-constituent"


class TestFmpMarketMoversCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_market_movers
    call_kwargs = {"type": "gainers"}
    route_url = f"{_BASE}/biggest-gainers"


class TestFmpScreenerCredentialDeclaration(CredentialDeclarationSuite):
    connector = fmp_screener
    call_kwargs: dict[str, str] = {}
    route_url = f"{_BASE}/company-screener"
