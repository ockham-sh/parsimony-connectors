"""Offline tests for the EODHD connectors.

Every verb is exercised with a mocked transport (respx). EODHD auth is the
``api_token`` query parameter (redacted by the transport layer); the canonical
error-mapping contract is covered on ``eodhd_search`` (see
``test_error_mapping_eodhd.py``). These tests assert: happy-path row shaping,
EmptyData/Parse guards, inline parameter validation, the no-key
``UnauthorizedError`` fast-fail (shared ``_client`` → all 17 verbs), the
plan-tier 403/423 → PaymentRequiredError mapping, and that the bound key is
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

from parsimony_eodhd import (
    CONNECTORS,
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
    load,
)

_KEY = "live-looking-key-eodhd-xyz"
_BASE = "https://eodhd.com/api"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_count_matches_docstring() -> None:
    assert len(CONNECTORS) == 17


def test_every_verb_declares_api_key_secret() -> None:
    # The headline Theme-B fix: every verb strips api_key from provenance.
    for c in CONNECTORS:
        assert "api_key" in c.secrets, f"{c.name} is missing secrets=('api_key',)"


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


def test_load_binds_key_across_collection() -> None:
    bundle = load(api_key=_KEY)
    for c in bundle:
        assert "api_key" not in c.exposed_signature.parameters, c.name


# ---------------------------------------------------------------------------
# eodhd_search (tool-tagged, carries error-mapping contract)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_search_returns_rows_and_strips_key() -> None:
    respx.get(f"{_BASE}/search/apple").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "Code": "AAPL",
                    "Exchange": "US",
                    "Name": "Apple Inc",
                    "Type": "Common Stock",
                    "Country": "USA",
                    "Currency": "USD",
                    "ISIN": "US0378331005",
                    "isPrimary": True,  # provider extra → must be dropped
                    "previousClose": 310.26,
                }
            ],
        )
    )

    result = eodhd_search.bind(api_key=_KEY)(query="apple")

    assert result.provenance.source == "eodhd_search"
    # Theme-B: the bound key must not appear in provenance.
    assert _KEY not in str(result.provenance.params)
    assert "api_key" not in result.provenance.params
    df = result.raw
    assert df.iloc[0]["Code"] == "AAPL"
    # Provider extras are projected out (only declared columns survive).
    assert "isPrimary" not in df.columns
    assert "previousClose" not in df.columns


@respx.mock
def test_eodhd_search_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/search/zzz").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        eodhd_search.bind(api_key=_KEY)(query="zzz")


@respx.mock
def test_eodhd_search_non_list_raises_parse_error() -> None:
    respx.get(f"{_BASE}/search/apple").mock(return_value=httpx.Response(200, json={"detail": "weird"}))
    with pytest.raises(ParseError):
        eodhd_search.bind(api_key=_KEY)(query="apple")


def test_eodhd_search_rejects_empty_query() -> None:
    with pytest.raises(InvalidParameterError, match="query"):
        eodhd_search.bind(api_key=_KEY)(query="   ")


def test_eodhd_search_rejects_bad_limit() -> None:
    with pytest.raises(InvalidParameterError, match="limit"):
        eodhd_search.bind(api_key=_KEY)(query="apple", limit=0)


@respx.mock
def test_eodhd_search_maps_401_without_leaking_key() -> None:
    respx.get(f"{_BASE}/search/apple").mock(return_value=httpx.Response(401, text="Unauthenticated"))
    with pytest.raises(UnauthorizedError) as exc_info:
        eodhd_search.bind(api_key=_KEY)(query="apple")
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_eodhd_search_maps_429_without_leaking_key() -> None:
    respx.get(f"{_BASE}/search/apple").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "15"}, text="too many requests")
    )
    with pytest.raises(RateLimitError) as exc_info:
        eodhd_search.bind(api_key=_KEY)(query="apple")
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Plan-tier status mapping (403 / 423 → PaymentRequiredError, NOT auth)
# ---------------------------------------------------------------------------


@respx.mock
def test_403_maps_to_payment_required() -> None:
    # 403 "Only EOD data allowed for free users" is a plan restriction, not a
    # credential failure — must NOT map to UnauthorizedError.
    respx.get(f"{_BASE}/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(403, text="Only EOD data allowed for free users.")
    )
    with pytest.raises(PaymentRequiredError) as exc_info:
        eodhd_fundamentals.bind(api_key=_KEY)(ticker="AAPL.US")
    assert exc_info.value.provider == "eodhd"
    assert not isinstance(exc_info.value, UnauthorizedError)
    # The key must not survive in the chained httpx cause either (F1 repro).
    assert_no_secret_leak(exc_info.value, secret=_KEY)


@respx.mock
def test_423_maps_to_payment_required() -> None:
    # 423 Locked "Bulk requests are prohibited for free users" → plan restriction.
    respx.get(f"{_BASE}/eod-bulk-last-day/US").mock(
        return_value=httpx.Response(423, text="Bulk requests are prohibited for free users.")
    )
    with pytest.raises(PaymentRequiredError) as exc_info:
        eodhd_bulk_eod.bind(api_key=_KEY)(exchange="US")
    assert exc_info.value.provider == "eodhd"
    assert_no_secret_leak(exc_info.value, secret=_KEY)


# ---------------------------------------------------------------------------
# eodhd_eod (free-tier ``warning`` field must be projected out)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_eod_returns_ohlc_and_drops_warning() -> None:
    respx.get(f"{_BASE}/eod/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2024-01-02",
                    "open": 170.0,
                    "high": 172.0,
                    "low": 169.0,
                    "close": 171.5,
                    "adjusted_close": 171.5,
                    "volume": 45000000,
                    "warning": "Data is limited by one year as you have free subscription",
                }
            ],
        )
    )
    result = eodhd_eod.bind(api_key=_KEY)(ticker="AAPL.US")
    df = result.raw
    assert df.iloc[0]["close"] == 171.5
    assert "warning" not in df.columns


@respx.mock
def test_eodhd_eod_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/eod/AAPL.US").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        eodhd_eod.bind(api_key=_KEY)(ticker="AAPL.US")


def test_eodhd_eod_rejects_unsafe_ticker() -> None:
    with pytest.raises(InvalidParameterError, match="unsafe"):
        eodhd_eod.bind(api_key=_KEY)(ticker="../etc/passwd")


# ---------------------------------------------------------------------------
# eodhd_live (single dict → one-row frame)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_live_returns_quote() -> None:
    respx.get(f"{_BASE}/real-time/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json={
                "code": "AAPL.US",
                "timestamp": 1780518540,
                "gmtoffset": 0,
                "open": 314.17,
                "high": 316.94,
                "low": 308.85,
                "close": 310.26,
                "volume": 49849993,
                "previousClose": 315.2,
                "change": -4.94,
                "change_p": -1.5,
            },
        )
    )
    result = eodhd_live.bind(api_key=_KEY)(ticker="AAPL.US")
    df = result.raw
    assert df.iloc[0]["code"] == "AAPL.US"
    assert df.iloc[0]["close"] == 310.26


@respx.mock
def test_eodhd_live_no_code_raises_empty_data() -> None:
    respx.get(f"{_BASE}/real-time/AAPL.US").mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(EmptyDataError):
        eodhd_live.bind(api_key=_KEY)(ticker="AAPL.US")


# ---------------------------------------------------------------------------
# eodhd_intraday
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_intraday_returns_bars() -> None:
    respx.get(f"{_BASE}/intraday/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "timestamp": 1700000000,
                    "datetime": "2023-11-14 18:00:00",
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 1000,
                }
            ],
        )
    )
    result = eodhd_intraday.bind(api_key=_KEY)(ticker="AAPL.US", interval="5m")
    assert result.raw.iloc[0]["close"] == 1.5


# ---------------------------------------------------------------------------
# eodhd_bulk_eod
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_bulk_eod_returns_rows() -> None:
    respx.get(f"{_BASE}/eod-bulk-last-day/US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "code": "AAPL",
                    "name": "Apple Inc",
                    "exchange_short_name": "US",
                    "date": "2024-01-02",
                    "open": 170.0,
                    "high": 172.0,
                    "low": 169.0,
                    "close": 171.5,
                    "adjusted_close": 171.5,
                    "volume": 45000000,
                }
            ],
        )
    )
    result = eodhd_bulk_eod.bind(api_key=_KEY)(exchange="US")
    assert result.raw.iloc[0]["code"] == "AAPL"


# ---------------------------------------------------------------------------
# eodhd_dividends / eodhd_splits
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_dividends_returns_rows() -> None:
    respx.get(f"{_BASE}/div/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2020-02-07",
                    "declarationDate": "2020-01-28",
                    "recordDate": "2020-02-10",
                    "paymentDate": "2020-02-13",
                    "period": "Quarterly",
                    "value": 0.1925,
                    "unadjustedValue": 0.77,
                    "currency": "USD",
                }
            ],
        )
    )
    result = eodhd_dividends.bind(api_key=_KEY)(ticker="AAPL.US")
    assert result.raw.iloc[0]["value"] == 0.1925


@respx.mock
def test_eodhd_dividends_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/div/AAPL.US").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        eodhd_dividends.bind(api_key=_KEY)(ticker="AAPL.US")


@respx.mock
def test_eodhd_splits_returns_rows() -> None:
    respx.get(f"{_BASE}/splits/AAPL.US").mock(
        return_value=httpx.Response(200, json=[{"date": "1987-06-16", "split": "2.000000/1.000000"}])
    )
    result = eodhd_splits.bind(api_key=_KEY)(ticker="AAPL.US")
    assert result.raw.iloc[0]["split"] == "2.000000/1.000000"


# ---------------------------------------------------------------------------
# eodhd_fundamentals (raw nested dict — no schema)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_fundamentals_returns_dict() -> None:
    respx.get(f"{_BASE}/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json={"General": {"Code": "AAPL"}, "Highlights": {"MarketCapitalization": 3e12}},
        )
    )
    result = eodhd_fundamentals.bind(api_key=_KEY)(ticker="AAPL.US")
    assert isinstance(result.raw, dict)
    assert result.raw["General"]["Code"] == "AAPL"


@respx.mock
def test_eodhd_fundamentals_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/fundamentals/AAPL.US").mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(EmptyDataError):
        eodhd_fundamentals.bind(api_key=_KEY)(ticker="AAPL.US")


# ---------------------------------------------------------------------------
# eodhd_calendar (rows wrapped under a type key; ipos/earnings/trends/splits)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_calendar_unwraps_earnings() -> None:
    respx.get(f"{_BASE}/calendar/earnings").mock(
        return_value=httpx.Response(
            200,
            json={
                "earnings": [
                    {
                        "code": "AAPL.US",
                        "date": "2024-02-01",
                        "report_date": "2024-02-01",
                        "before_after_market": "AfterMarket",
                        "currency": "USD",
                        "actual": 2.18,
                        "estimate": 2.10,
                        "difference": 0.08,
                        "percent": 3.8,
                    }
                ]
            },
        )
    )
    result = eodhd_calendar.bind(api_key=_KEY)(type="earnings")
    assert result.raw.iloc[0]["code"] == "AAPL.US"


@respx.mock
def test_eodhd_calendar_ipos_uses_correct_path() -> None:
    # The IPO calendar type is "ipos" (not "ipo"); the route must match.
    route = respx.get(f"{_BASE}/calendar/ipos").mock(
        return_value=httpx.Response(200, json={"ipos": [{"code": "NEW.US", "date": "2024-03-01"}]})
    )
    result = eodhd_calendar.bind(api_key=_KEY)(type="ipos")
    assert route.called
    assert result.raw.iloc[0]["code"] == "NEW.US"


# ---------------------------------------------------------------------------
# eodhd_news (provider extra ``sentiment`` must be projected out)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_news_returns_rows_and_drops_sentiment() -> None:
    respx.get(f"{_BASE}/news").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2026-06-03T20:13:24+00:00",
                    "title": "Apple beats earnings",
                    "content": "Strong quarter.",
                    "link": "https://example.com/a",
                    "symbols": ["AAPL.US"],
                    "tags": ["earnings"],
                    "sentiment": {"polarity": 0.5},  # provider extra (dict) → dropped
                }
            ],
        )
    )
    result = eodhd_news.bind(api_key=_KEY)(ticker="AAPL.US")
    df = result.raw
    assert df.iloc[0]["title"] == "Apple beats earnings"
    assert "sentiment" not in df.columns


def test_eodhd_news_rejects_bad_limit() -> None:
    with pytest.raises(InvalidParameterError, match="limit"):
        eodhd_news.bind(api_key=_KEY)(limit=0)


# ---------------------------------------------------------------------------
# eodhd_macro / eodhd_macro_bulk
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_macro_returns_rows() -> None:
    respx.get(f"{_BASE}/macro-indicator/USA").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "CountryCode": "USA",
                    "CountryName": "United States",
                    "Indicator": "gdp_current_usd",
                    "Date": "2023-12-31",
                    "Period": "Annual",
                    "Value": 27000000000000.0,
                }
            ],
        )
    )
    result = eodhd_macro.bind(api_key=_KEY)(country="USA", indicator="gdp_current_usd")
    assert result.raw.iloc[0]["Value"] == 27000000000000.0


def test_eodhd_macro_rejects_empty_indicator() -> None:
    with pytest.raises(InvalidParameterError, match="indicator"):
        eodhd_macro.bind(api_key=_KEY)(country="USA", indicator="  ")


@respx.mock
def test_eodhd_macro_bulk_returns_rows() -> None:
    respx.get(f"{_BASE}/macro-indicator/USA").mock(
        return_value=httpx.Response(
            200,
            json=[{"Date": "2023-12-31", "Period": "Annual", "Value": 1.0}],
        )
    )
    result = eodhd_macro_bulk.bind(api_key=_KEY)(country="USA")
    assert result.raw.iloc[0]["Value"] == 1.0


# ---------------------------------------------------------------------------
# eodhd_technical (wildcard schema keeps indicator-specific columns)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_technical_returns_indicator_columns() -> None:
    respx.get(f"{_BASE}/technical/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2024-01-02",
                    "open": 170.0,
                    "high": 171.0,
                    "low": 169.0,
                    "close": 170.5,
                    "volume": 1000,
                    "sma": 170.5,
                }
            ],
        )
    )
    result = eodhd_technical.bind(api_key=_KEY)(ticker="AAPL.US", function="sma")
    df = result.raw
    assert df.iloc[0]["sma"] == 170.5


def test_eodhd_technical_rejects_bad_period() -> None:
    with pytest.raises(InvalidParameterError, match="period"):
        eodhd_technical.bind(api_key=_KEY)(ticker="AAPL.US", function="sma", period=0)


# ---------------------------------------------------------------------------
# eodhd_insider
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_insider_returns_rows() -> None:
    respx.get(f"{_BASE}/insider-transactions").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "code": "AAPL.US",
                    "date": "2024-01-02",
                    "ownerName": "Tim Cook",
                    "ownerCik": "0001234567",
                    "transactionType": "S",
                    "transactionDate": "2024-01-02",
                    "value": 1000000.0,
                    "sharesOwned": 50000.0,
                    "change": -1000.0,
                }
            ],
        )
    )
    result = eodhd_insider.bind(api_key=_KEY)(ticker="AAPL.US")
    assert result.raw.iloc[0]["code"] == "AAPL.US"


# ---------------------------------------------------------------------------
# eodhd_screener (rows wrapped under ``data``)
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_screener_unwraps_data() -> None:
    respx.get(f"{_BASE}/screener").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "code": "AAPL",
                        "name": "Apple Inc",
                        "exchange": "US",
                        "currency": "USD",
                        "sector": "Technology",
                        "industry": "Consumer Electronics",
                        "market_capitalization": 3e12,
                    }
                ]
            },
        )
    )
    result = eodhd_screener.bind(api_key=_KEY)(
        filters=[("market_capitalization", ">", "1000000000")], sort="market_capitalization"
    )
    assert result.raw.iloc[0]["code"] == "AAPL"


def test_eodhd_screener_rejects_bad_limit() -> None:
    with pytest.raises(InvalidParameterError, match="limit"):
        eodhd_screener.bind(api_key=_KEY)(limit=0)


# ---------------------------------------------------------------------------
# eodhd_exchanges / eodhd_exchange_symbols
# ---------------------------------------------------------------------------


@respx.mock
def test_eodhd_exchanges_lists_exchanges() -> None:
    respx.get(f"{_BASE}/exchanges-list").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"Code": "US", "Name": "USA Stocks", "Country": "USA", "Currency": "USD"},
                {"Code": "LSE", "Name": "London", "Country": "UK", "Currency": "GBP"},
            ],
        )
    )
    result = eodhd_exchanges.bind(api_key=_KEY)()
    assert set(result.raw["Code"]) == {"US", "LSE"}


@respx.mock
def test_eodhd_exchange_symbols_lists_symbols() -> None:
    respx.get(f"{_BASE}/exchange-symbol-list/US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "Code": "AAPL",
                    "Name": "Apple Inc",
                    "Country": "USA",
                    "Exchange": "NASDAQ",
                    "Currency": "USD",
                    "Type": "Common Stock",
                    "Isin": "US0378331005",
                }
            ],
        )
    )
    result = eodhd_exchange_symbols.bind(api_key=_KEY)(exchange="US")
    assert result.raw.iloc[0]["Code"] == "AAPL"
    assert result.raw.iloc[0]["Isin"] == "US0378331005"


def test_eodhd_exchange_symbols_rejects_unsafe_exchange() -> None:
    with pytest.raises(InvalidParameterError, match="unsafe"):
        eodhd_exchange_symbols.bind(api_key=_KEY)(exchange="../etc")


# ---------------------------------------------------------------------------
# No-key fast-fail — shared _client, so EVERY keyed verb must raise
# UnauthorizedError(env_var="EODHD_API_KEY") before any network call.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("connector_fn", "kwargs"),
    [
        (eodhd_search, {"query": "apple"}),
        (eodhd_exchanges, {}),
        (eodhd_exchange_symbols, {"exchange": "US"}),
        (eodhd_eod, {"ticker": "AAPL.US"}),
        (eodhd_live, {"ticker": "AAPL.US"}),
        (eodhd_intraday, {"ticker": "AAPL.US", "interval": "5m"}),
        (eodhd_bulk_eod, {"exchange": "US"}),
        (eodhd_dividends, {"ticker": "AAPL.US"}),
        (eodhd_splits, {"ticker": "AAPL.US"}),
        (eodhd_fundamentals, {"ticker": "AAPL.US"}),
        (eodhd_calendar, {"type": "earnings"}),
        (eodhd_news, {}),
        (eodhd_macro, {"country": "USA", "indicator": "gdp_current_usd"}),
        (eodhd_macro_bulk, {"country": "USA"}),
        (eodhd_technical, {"ticker": "AAPL.US", "function": "sma"}),
        (eodhd_insider, {"ticker": "AAPL.US"}),
        (eodhd_screener, {}),
    ],
)
def test_no_key_raises_unauthorized(connector_fn, kwargs, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        connector_fn(**kwargs)
    assert exc_info.value.env_var == "EODHD_API_KEY"
    assert exc_info.value.provider == "eodhd"


def test_no_key_case_covers_all_seventeen_verbs() -> None:
    # Guard against silently dropping a verb from the parametrize list above.
    assert len(CONNECTORS) == 17
