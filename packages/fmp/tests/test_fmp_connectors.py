"""Offline tests for the FMP connectors.

Every verb is exercised with a mocked transport (respx). FMP auth is the
``apikey`` query parameter (redacted by the transport layer); the canonical
error-mapping contract is covered on ``fmp_search`` (see
``test_error_mapping_fmp.py``). These tests assert: happy-path row shaping,
EmptyData/Parse guards, inline parameter validation, the no-key
``UnauthorizedError`` fast-fail (shared ``_client`` → all 19 verbs), the
plan-tier 402/403 → PaymentRequiredError mapping, and that the bound key is
stripped from provenance (the Theme-B secrets fix).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony_test_support import assert_no_secret_leak

from parsimony_fmp import (
    CONNECTORS,
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
    load,
)

_KEY = "live-looking-fmp-key-do-not-leak"
_BASE = "https://financialmodelingprep.com/stable"


# ---------------------------------------------------------------------------
# Plugin contract shape (Theme-B headline: secrets on every verb)
# ---------------------------------------------------------------------------


def test_connectors_count() -> None:
    assert len(CONNECTORS) == 19


def test_every_verb_declares_api_key_secret() -> None:
    # The headline Theme-B fix: every verb strips api_key from provenance.
    for c in CONNECTORS:
        assert "api_key" in c.secrets, f"{c.name} is missing secrets=('api_key',)"


def test_load_binds_key_across_collection() -> None:
    bundle = load(api_key=_KEY)
    for c in bundle:
        assert "api_key" not in c.exposed_signature.parameters, c.name


# ---------------------------------------------------------------------------
# fmp_search (tool-tagged; carries error-mapping contract; strips the key)
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_search_returns_rows_and_strips_key() -> None:
    respx.get(f"{_BASE}/search-name").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc",
                    "currency": "USD",
                    "exchangeFullName": "NASDAQ",
                    "exchange": "NASDAQ",
                    "isPrimary": True,  # provider extra → must be dropped
                }
            ],
        )
    )
    result = fmp_search.bind(api_key=_KEY)(query="apple")

    assert result.provenance.source == "fmp_search"
    # Theme-B: the bound key must not appear in provenance.
    assert _KEY not in str(result.provenance.params)
    assert "api_key" not in result.provenance.params
    df = result.data
    assert df.iloc[0]["symbol"] == "AAPL"
    assert "isPrimary" not in df.columns


@respx.mock
def test_fmp_search_empty_raises_empty_data() -> None:
    # FMP returns 200 with [] for an unknown query.
    respx.get(f"{_BASE}/search-name").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        fmp_search.bind(api_key=_KEY)(query="zzzznotacompany")


@respx.mock
def test_fmp_search_non_list_raises_parse_error() -> None:
    respx.get(f"{_BASE}/search-name").mock(return_value=httpx.Response(200, json="weird"))
    with pytest.raises(ParseError):
        fmp_search.bind(api_key=_KEY)(query="apple")


def test_fmp_search_rejects_empty_query() -> None:
    with pytest.raises(InvalidParameterError, match="query"):
        fmp_search.bind(api_key=_KEY)(query="   ")


# ---------------------------------------------------------------------------
# fmp_taxonomy (enum dispatch; un-shaped DataFrame)
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_taxonomy_sectors() -> None:
    respx.get(f"{_BASE}/available-sectors").mock(
        return_value=httpx.Response(200, json=[{"sector": "Technology"}, {"sector": "Energy"}])
    )
    result = fmp_taxonomy.bind(api_key=_KEY)(type="sectors")
    assert set(result.data["sector"]) == {"Technology", "Energy"}


# ---------------------------------------------------------------------------
# fmp_quotes (batch-quote; declares only columns the payload carries)
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_quotes_returns_rows() -> None:
    respx.get(f"{_BASE}/batch-quote").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "price": 310.26,
                    "changePercentage": -1.56,
                    "change": -4.94,
                    "dayLow": 308.85,
                    "dayHigh": 316.94,
                    "yearLow": 195.07,
                    "yearHigh": 316.94,
                    "marketCap": 4_556_899_072_560,
                    "volume": 50_459_550,
                    "priceAvg50": 250.0,
                    "priceAvg200": 240.0,
                    "exchange": "NASDAQ",
                    "open": 312.0,
                    "previousClose": 315.2,
                    "timestamp": 1780518540,  # provider extra → dropped
                }
            ],
        )
    )
    result = fmp_quotes.bind(api_key=_KEY)(symbols="AAPL")
    df = result.data
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df.iloc[0]["changePercentage"] == -1.56
    # the schema declares only columns the live payload carries
    assert "timestamp" not in df.columns
    assert "pe" not in df.columns


def test_fmp_quotes_rejects_empty_symbols() -> None:
    with pytest.raises(InvalidParameterError, match="symbols"):
        fmp_quotes.bind(api_key=_KEY)(symbols="  ")


# ---------------------------------------------------------------------------
# fmp_prices (daily + intraday path branching; datetime preserves time)
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_prices_daily_returns_ohlcv() -> None:
    respx.get(f"{_BASE}/historical-price-eod/full").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "date": "2025-05-05",
                    "open": 203.1,
                    "high": 204.1,
                    "low": 198.21,
                    "close": 198.89,
                    "volume": 69018500,
                    "change": -4.21,
                    "changePercent": -2.07,
                    "vwap": 200.0,
                }
            ],
        )
    )
    result = fmp_prices.bind(api_key=_KEY)(symbol="AAPL", frequency="daily")
    df = result.data
    assert df.iloc[0]["close"] == 198.89
    # symbol is a provider extra here (prices schema is date-keyed, no symbol col)
    assert "symbol" not in df.columns


@respx.mock
def test_fmp_prices_dividend_adjusted_renames_adj_columns() -> None:
    """Regression: the dividend-adjusted route returns adjOpen/adjHigh/adjLow/adjClose
    (no open/high/low/close). They must be renamed onto the declared schema before
    shaping, otherwise _select_declared drops every price column."""
    respx.get(f"{_BASE}/historical-price-eod/dividend-adjusted").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "date": "2025-05-09",
                    "adjOpen": 197.95,
                    "adjHigh": 199.48,
                    "adjLow": 196.5,
                    "adjClose": 197.48,
                    "volume": 36453923,
                }
            ],
        )
    )
    result = fmp_prices.bind(api_key=_KEY)(symbol="AAPL", frequency="dividend_adjusted")
    df = result.data
    assert "close" in df.columns, f"close dropped — columns are {list(df.columns)}"
    assert df.iloc[0]["close"] == 197.48
    assert df.iloc[0]["open"] == 197.95
    assert df.iloc[0]["high"] == 199.48
    assert df.iloc[0]["low"] == 196.5
    assert df.iloc[0]["volume"] == 36453923
    # The route carries neither the raw adj* names (renamed) nor change/vwap.
    assert "adjClose" not in df.columns


@respx.mock
def test_fmp_prices_intraday_preserves_time_component() -> None:
    """Regression: fmp_prices must parse `date` as datetime WITHOUT normalizing,
    otherwise intraday times would be zeroed out."""
    respx.get(f"{_BASE}/historical-chart/1min").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"date": "2026-05-08 14:30:00", "open": 200.1, "high": 200.5, "low": 200.0, "close": 200.4},
                {"date": "2026-05-08 14:29:00", "open": 199.9, "high": 200.2, "low": 199.9, "close": 200.1},
            ],
        )
    )
    result = fmp_prices.bind(api_key=_KEY)(symbol="AAPL", frequency="1min")
    times = result.data["date"].dt.time.astype(str).tolist()
    assert "00:00:00" not in times
    assert times[0] == "14:30:00"


def test_fmp_prices_rejects_bad_frequency() -> None:
    with pytest.raises(InvalidParameterError, match="frequency"):
        fmp_prices.bind(api_key=_KEY)(symbol="AAPL", frequency="monthly")


# ---------------------------------------------------------------------------
# fmp_company_profile / fmp_peers
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_company_profile_returns_row_and_drops_extras() -> None:
    respx.get(f"{_BASE}/profile").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "companyName": "Apple Inc.",
                    "price": 310.26,
                    "marketCap": 4.5e12,
                    "beta": 1.06,
                    "exchange": "NASDAQ",
                    "exchangeFullName": "NASDAQ Global Select",
                    "currency": "USD",
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "country": "US",
                    "fullTimeEmployees": 164000,
                    "ceo": "Timothy Cook",
                    "description": "Apple designs...",
                    "website": "https://apple.com",
                    "ipoDate": "1980-12-12",
                    "isEtf": False,
                    "isActivelyTrading": True,
                    "isAdr": False,
                    "isFund": False,
                    "cusip": "037833100",  # extra → dropped
                }
            ],
        )
    )
    result = fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    df = result.data
    assert df.iloc[0]["companyName"] == "Apple Inc."
    assert "cusip" not in df.columns


@respx.mock
def test_fmp_company_profile_unknown_symbol_raises_empty_data() -> None:
    respx.get(f"{_BASE}/profile").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        fmp_company_profile.bind(api_key=_KEY)(symbol="ZZZZZZ")


@respx.mock
def test_fmp_peers_returns_peer_group() -> None:
    respx.get(f"{_BASE}/stock-peers").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"symbol": "GOOGL", "companyName": "Alphabet Inc.", "price": 358.99, "mktCap": 4.3e12},
                {"symbol": "META", "companyName": "Meta Platforms", "price": 700.0, "mktCap": 1.8e12},
            ],
        )
    )
    result = fmp_peers.bind(api_key=_KEY)(symbol="AAPL")
    assert set(result.data["symbol"]) == {"GOOGL", "META"}


# ---------------------------------------------------------------------------
# Statements (income / balance / cash flow) — extras projected/wildcarded
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_income_statements_projects_declared_columns() -> None:
    respx.get(f"{_BASE}/income-statement").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2025-09-27",
                    "symbol": "AAPL",
                    "reportedCurrency": "USD",
                    "cik": "0000320193",  # extra → dropped (no wildcard)
                    "fiscalYear": "2025",
                    "revenue": 4e11,
                    "costOfRevenue": 2e11,
                    "grossProfit": 2e11,
                    "operatingExpenses": 6e10,
                    "operatingIncome": 1.4e11,
                    "ebitda": 1.5e11,
                    "netIncome": 1e11,
                    "eps": 6.5,
                    "epsDiluted": 6.4,
                }
            ],
        )
    )
    result = fmp_income_statements.bind(api_key=_KEY)(symbol="AAPL", limit=1)
    df = result.data
    assert df.iloc[0]["revenue"] == 4e11
    assert "cik" not in df.columns
    assert "fiscalYear" not in df.columns


@respx.mock
def test_fmp_balance_sheet_keeps_extras_via_wildcard() -> None:
    respx.get(f"{_BASE}/balance-sheet-statement").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2025-09-27",
                    "symbol": "AAPL",
                    "totalAssets": 3.5e11,
                    "totalLiabilities": 2.8e11,
                    "totalStockholdersEquity": 7e10,
                    "totalDebt": 1e11,
                    "netDebt": 9e10,
                    "cashAndCashEquivalents": 3e10,
                    "goodwill": 0,  # extra kept by wildcard
                }
            ],
        )
    )
    result = fmp_balance_sheet_statements.bind(api_key=_KEY)(symbol="AAPL", limit=1)
    df = result.data
    assert df.iloc[0]["totalAssets"] == 3.5e11
    assert "goodwill" in df.columns  # wildcard keeps it


@respx.mock
def test_fmp_cash_flow_statements_returns_rows() -> None:
    respx.get(f"{_BASE}/cash-flow-statement").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2025-09-27",
                    "symbol": "AAPL",
                    "reportedCurrency": "USD",
                    "netIncome": 1e11,
                    "operatingCashFlow": 1.2e11,
                    "capitalExpenditure": -1e10,
                    "freeCashFlow": 1.1e11,
                    "netCashProvidedByOperatingActivities": 1.2e11,
                    "netCashProvidedByInvestingActivities": -2e10,
                    "netCashProvidedByFinancingActivities": -9e10,
                    "netChangeInCash": 1e10,
                }
            ],
        )
    )
    result = fmp_cash_flow_statements.bind(api_key=_KEY)(symbol="AAPL", limit=1)
    assert result.data.iloc[0]["freeCashFlow"] == 1.1e11


# ---------------------------------------------------------------------------
# Events & catalysts
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_corporate_history_earnings() -> None:
    respx.get(f"{_BASE}/earnings").mock(
        return_value=httpx.Response(
            200,
            json=[{"symbol": "AAPL", "date": "2026-07-30", "epsActual": None, "epsEstimated": 1.86}],
        )
    )
    result = fmp_corporate_history.bind(api_key=_KEY)(symbol="AAPL", event_type="earnings")
    assert result.data.iloc[0]["symbol"] == "AAPL"


@respx.mock
def test_fmp_event_calendar_dividends() -> None:
    respx.get(f"{_BASE}/dividends-calendar").mock(
        return_value=httpx.Response(200, json=[{"symbol": "AAPL", "date": "2026-06-05", "dividend": 0.27}])
    )
    result = fmp_event_calendar.bind(api_key=_KEY)(event_type="dividends", from_date="2026-06-01", to_date="2026-06-10")
    assert result.data.iloc[0]["dividend"] == 0.27


@respx.mock
def test_fmp_analyst_estimates_returns_rows() -> None:
    respx.get(f"{_BASE}/analyst-estimates").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "date": "2030-09-27",
                    "revenueLow": 6e11,
                    "revenueAvg": 6.2e11,
                    "revenueHigh": 6.6e11,
                    "ebitdaLow": 2.1e11,
                    "ebitdaAvg": 2.2e11,
                    "ebitdaHigh": 2.3e11,
                    "netIncomeLow": 1.5e11,
                    "netIncomeAvg": 1.6e11,
                    "netIncomeHigh": 1.7e11,
                    "epsLow": 9.0,
                    "epsAvg": 9.5,
                    "epsHigh": 10.0,
                    "numAnalystsRevenue": 12,
                    "numAnalystsEps": 14,
                }
            ],
        )
    )
    result = fmp_analyst_estimates.bind(api_key=_KEY)(symbol="AAPL")
    assert result.data.iloc[0]["epsAvg"] == 9.5


# ---------------------------------------------------------------------------
# Signals & context
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_news_returns_articles() -> None:
    respx.get(f"{_BASE}/news/stock").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "publishedDate": "2026-06-03 18:02:24",
                    "title": "Apple beats earnings",
                    "text": "Strong quarter.",
                    "url": "https://example.com/a",
                    "site": "Investopedia",
                    "image": "https://example.com/i.png",
                    "publisher": "Investopedia",  # extra → dropped
                }
            ],
        )
    )
    result = fmp_news.bind(api_key=_KEY)(type="news", symbols="AAPL")
    df = result.data
    assert df.iloc[0]["title"] == "Apple beats earnings"
    assert "publisher" not in df.columns


def test_fmp_news_rejects_empty_symbols() -> None:
    with pytest.raises(InvalidParameterError, match="symbols"):
        fmp_news.bind(api_key=_KEY)(type="news", symbols=" ")


@respx.mock
def test_fmp_insider_trades_returns_rows() -> None:
    respx.get(f"{_BASE}/insider-trading/search").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "filingDate": "2026-05-29",
                    "transactionDate": "2026-05-27",
                    "reportingName": "Cook Timothy",
                    "typeOfOwner": "officer: CEO",
                    "transactionType": "S-Sale",
                    "acquisitionOrDisposition": "D",
                    "securitiesTransacted": 1000,
                    "price": 310.0,
                    "securitiesOwned": 5000,
                    "formType": "4",
                    "url": "https://sec.gov/x",
                }
            ],
        )
    )
    result = fmp_insider_trades.bind(api_key=_KEY)(symbol="AAPL")
    assert result.data.iloc[0]["reportingName"] == "Cook Timothy"


@respx.mock
def test_fmp_institutional_positions_returns_row() -> None:
    respx.get(f"{_BASE}/institutional-ownership/symbol-positions-summary").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "date": "2024-03-31",
                    "investorsHolding": 5272,
                    "investorsHoldingChange": 56,
                    "numberOf13Fshares": 9_000_000_000,
                    "numberOf13FsharesChange": 1_000_000,
                    "totalInvested": 2.5e12,
                    "totalInvestedChange": 1e10,
                    "ownershipPercent": 60.5,
                    "ownershipPercentChange": 0.3,
                    "newPositions": 100,
                    "closedPositions": 50,
                    "increasedPositions": 800,
                    "reducedPositions": 700,
                    "putCallRatio": 0.8,
                }
            ],
        )
    )
    result = fmp_institutional_positions.bind(api_key=_KEY)(symbol="AAPL", year="2024", quarter="1")
    assert result.data.iloc[0]["investorsHolding"] == 5272


def test_fmp_institutional_positions_rejects_bad_quarter() -> None:
    with pytest.raises(InvalidParameterError, match="quarter"):
        fmp_institutional_positions.bind(api_key=_KEY)(symbol="AAPL", year="2024", quarter="5")


@respx.mock
def test_fmp_earnings_transcript_returns_text() -> None:
    respx.get(f"{_BASE}/earning-call-transcript").mock(
        return_value=httpx.Response(
            200,
            json=[{"symbol": "AAPL", "period": "Q1", "year": 2024, "date": "2024-02-01", "content": "Operator: ..."}],
        )
    )
    result = fmp_earnings_transcript.bind(api_key=_KEY)(symbol="AAPL", year="2024", quarter="1")
    assert result.data.iloc[0]["content"].startswith("Operator")


def test_fmp_earnings_transcript_rejects_bad_quarter() -> None:
    with pytest.raises(InvalidParameterError, match="quarter"):
        fmp_earnings_transcript.bind(api_key=_KEY)(symbol="AAPL", year="2024", quarter="0")


# ---------------------------------------------------------------------------
# Market context
# ---------------------------------------------------------------------------


@respx.mock
def test_fmp_index_constituents_sp500() -> None:
    respx.get(f"{_BASE}/sp500-constituent").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "sector": "Technology",
                    "subSector": "Consumer Electronics",
                    "headQuarter": "Cupertino, CA",
                    "dateFirstAdded": "1982-11-30",
                    "cik": "0000320193",
                    "founded": "1976-04-01",
                }
            ],
        )
    )
    result = fmp_index_constituents.bind(api_key=_KEY)(index="SP500")
    assert result.data.iloc[0]["symbol"] == "AAPL"


@respx.mock
def test_fmp_market_movers_gainers() -> None:
    respx.get(f"{_BASE}/biggest-gainers").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "XOS",
                    "name": "Xos, Inc.",
                    "price": 7.46,
                    "change": 5.23,
                    "changesPercentage": 234.5,
                    "exchange": "NASDAQ",
                }
            ],
        )
    )
    result = fmp_market_movers.bind(api_key=_KEY)(type="gainers")
    assert result.data.iloc[0]["symbol"] == "XOS"
    assert result.data.iloc[0]["changesPercentage"] == 234.5


# ---------------------------------------------------------------------------
# Plan-tier status mapping (402 / 403 → PaymentRequiredError, NOT auth)
# ---------------------------------------------------------------------------


@respx.mock
def test_403_maps_to_payment_required() -> None:
    # 403 "Legacy Endpoint / plan restriction" must NOT map to UnauthorizedError.
    respx.get(f"{_BASE}/profile").mock(
        return_value=httpx.Response(403, json={"Error Message": "Legacy Endpoint : ..."})
    )
    with pytest.raises(PaymentRequiredError) as exc_info:
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert exc_info.value.provider == "fmp"
    assert not isinstance(exc_info.value, UnauthorizedError)
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_402_maps_to_payment_required() -> None:
    respx.get(f"{_BASE}/profile").mock(return_value=httpx.Response(402, text="payment required"))
    with pytest.raises(PaymentRequiredError) as exc_info:
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert exc_info.value.provider == "fmp"
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_401_maps_to_unauthorized() -> None:
    # Invalid key is unambiguously 401 — must map to UnauthorizedError, not Payment.
    respx.get(f"{_BASE}/profile").mock(return_value=httpx.Response(401, json={"Error Message": "Invalid API KEY. ..."}))
    with pytest.raises(UnauthorizedError) as exc_info:
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert not isinstance(exc_info.value, PaymentRequiredError)
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Free-tier throttle disambiguation (F3): FMP overloads 403 for BOTH a plan
# restriction AND a rolling free-tier quota. The quota body says "Limit Reach";
# it is a *temporary* condition and must surface as a retry-able RateLimitError,
# not a terminal PaymentRequiredError (which makes an unattended agent give up
# permanently on a transient throttle). Status alone can't tell them apart.
# ---------------------------------------------------------------------------

_LIMIT_BODY = {"Error Message": "Limit Reach . Please upgrade your plan or visit our documentation."}


@respx.mock
def test_403_limit_reach_maps_to_rate_limit() -> None:
    respx.get(f"{_BASE}/profile").mock(return_value=httpx.Response(403, json=_LIMIT_BODY))
    with pytest.raises(RateLimitError) as exc_info:
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert not isinstance(exc_info.value, PaymentRequiredError)
    assert exc_info.value.provider == "fmp"
    # A retry-able error must carry something to schedule against.
    assert exc_info.value.retry_after is not None
    assert exc_info.value.retry_after > 0
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_403_limit_reach_is_case_insensitive() -> None:
    respx.get(f"{_BASE}/profile").mock(
        return_value=httpx.Response(403, json={"Error Message": "LIMIT REACHED for the day"})
    )
    with pytest.raises(RateLimitError):
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")


@respx.mock
def test_403_plan_restriction_is_not_a_throttle() -> None:
    # A genuine plan/legacy restriction has no quota language → stays terminal.
    respx.get(f"{_BASE}/profile").mock(
        return_value=httpx.Response(403, json={"Error Message": "Legacy Endpoint : ..."})
    )
    with pytest.raises(PaymentRequiredError) as exc_info:
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert not isinstance(exc_info.value, RateLimitError)
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.parametrize("body", [{"Error Message": "Limit Reach ."}, {"Error Message": "Legacy Endpoint : ..."}])
def test_403_does_not_leak_key_through_chained_exception(body: dict[str, str]) -> None:
    # Both the quota and plan 403s are raised fresh from the response status
    # (read via resp.status_code / resp.text), never from a raised httpx error
    # carrying request.url — so the apikey on the query string can never reach a
    # traceback / logging.exception. Status errors have no __cause__.
    respx.get(f"{_BASE}/profile").mock(return_value=httpx.Response(403, json=body))
    with pytest.raises((RateLimitError, PaymentRequiredError)) as exc_info:
        fmp_company_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert_no_secret_leak(exc_info.value, secret=_KEY)


# ---------------------------------------------------------------------------
# No-key fast-fail — shared _client, so EVERY keyed verb must raise
# UnauthorizedError(env_var="FMP_API_KEY") before any network call.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("connector_fn", "kwargs"),
    [
        (fmp_search, {"query": "apple"}),
        (fmp_taxonomy, {"type": "sectors"}),
        (fmp_quotes, {"symbols": "AAPL"}),
        (fmp_prices, {"symbol": "AAPL"}),
        (fmp_company_profile, {"symbol": "AAPL"}),
        (fmp_peers, {"symbol": "AAPL"}),
        (fmp_income_statements, {"symbol": "AAPL"}),
        (fmp_balance_sheet_statements, {"symbol": "AAPL"}),
        (fmp_cash_flow_statements, {"symbol": "AAPL"}),
        (fmp_corporate_history, {"symbol": "AAPL", "event_type": "earnings"}),
        (fmp_event_calendar, {"event_type": "earnings"}),
        (fmp_analyst_estimates, {"symbol": "AAPL"}),
        (fmp_news, {"type": "news", "symbols": "AAPL"}),
        (fmp_insider_trades, {"symbol": "AAPL"}),
        (fmp_institutional_positions, {"symbol": "AAPL", "year": "2024", "quarter": "1"}),
        (fmp_earnings_transcript, {"symbol": "AAPL", "year": "2024", "quarter": "1"}),
        (fmp_index_constituents, {"index": "SP500"}),
        (fmp_market_movers, {"type": "gainers"}),
        (fmp_screener, {"sector": "Technology"}),
    ],
)
def test_no_key_raises_unauthorized(connector_fn, kwargs, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        connector_fn(**kwargs)
    assert exc_info.value.env_var == "FMP_API_KEY"
    assert exc_info.value.provider == "fmp"


def test_no_key_case_covers_all_nineteen_verbs() -> None:
    # Guard against silently dropping a verb from the parametrize list above.
    assert len(CONNECTORS) == 19
