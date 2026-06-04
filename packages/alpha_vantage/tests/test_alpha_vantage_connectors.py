"""Offline tests for the Alpha Vantage connectors.

Alpha Vantage exposes 29 verbs through a single ``/query`` endpoint
differentiated by a ``function`` query param (CSV endpoints share the URL but
return CSV/text). Auth is the ``apikey`` query param (redacted by the transport
layer). The canonical HTTP-error-mapping contract is covered separately in
``test_error_mapping_alpha_vantage.py``; this file covers:

* happy-path row shaping for EVERY verb,
* the §5.8 HTTP-200 in-body error envelopes (Error Message / Note / Information),
* the no-key ``UnauthorizedError`` fast-fail parametrized over all 29 verbs,
* the Theme-B fix — every verb declares ``secrets=("api_key",)`` and strips the
  key from provenance,
* the FX/crypto synthetic-KEY injection (the raw payload carries no pair/symbol),
* the bounded enumerator.
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

from parsimony_alpha_vantage import CONNECTORS, load
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

_KEY = "live-looking-av-key-xyz"
_URL = "https://www.alphavantage.co/query"


def _mock(body: dict | None = None, *, text: str | None = None, status: int = 200) -> None:
    if text is not None:
        respx.get(_URL).mock(return_value=httpx.Response(status, text=text))
    else:
        respx.get(_URL).mock(return_value=httpx.Response(status, json=body))


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_count() -> None:
    assert len(CONNECTORS) == 29


def test_every_verb_declares_api_key_secret() -> None:
    # Theme-B headline fix: every verb strips api_key from provenance.
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
# No-key UnauthorizedError fast-fail — parametrized over ALL 29 verbs.
# Minimal valid kwargs per verb; the _client fast-fail fires before any network.
# ---------------------------------------------------------------------------

_NO_KEY_CASES = [
    (alpha_vantage_search, {"keywords": "apple"}),
    (alpha_vantage_quote, {"symbol": "IBM"}),
    (alpha_vantage_daily, {"symbol": "IBM"}),
    (alpha_vantage_weekly, {"symbol": "IBM"}),
    (alpha_vantage_monthly, {"symbol": "IBM"}),
    (alpha_vantage_intraday, {"symbol": "IBM"}),
    (alpha_vantage_overview, {"symbol": "IBM"}),
    (alpha_vantage_income_statement, {"symbol": "IBM"}),
    (alpha_vantage_balance_sheet, {"symbol": "IBM"}),
    (alpha_vantage_cash_flow, {"symbol": "IBM"}),
    (alpha_vantage_earnings, {"symbol": "IBM"}),
    (alpha_vantage_etf_profile, {"symbol": "SPY"}),
    (alpha_vantage_earnings_calendar, {}),
    (alpha_vantage_ipo_calendar, {}),
    (alpha_vantage_fx_rate, {"from_currency": "USD", "to_currency": "EUR"}),
    (alpha_vantage_fx_daily, {"from_symbol": "EUR", "to_symbol": "USD"}),
    (alpha_vantage_fx_weekly, {"from_symbol": "EUR", "to_symbol": "USD"}),
    (alpha_vantage_fx_monthly, {"from_symbol": "EUR", "to_symbol": "USD"}),
    (alpha_vantage_crypto_daily, {"symbol": "BTC"}),
    (alpha_vantage_crypto_weekly, {"symbol": "BTC"}),
    (alpha_vantage_crypto_monthly, {"symbol": "BTC"}),
    (alpha_vantage_econ, {"function": "REAL_GDP"}),
    (alpha_vantage_metal_spot, {"symbol": "GOLD"}),
    (alpha_vantage_metal_history, {"symbol": "GOLD"}),
    (alpha_vantage_news, {}),
    (alpha_vantage_top_movers, {}),
    (alpha_vantage_options, {"symbol": "IBM"}),
    (alpha_vantage_technical, {"symbol": "IBM", "function": "SMA"}),
    (enumerate_alpha_vantage, {}),
]


def test_no_key_cases_cover_every_verb() -> None:
    assert len(_NO_KEY_CASES) == 29
    assert {fn.name for fn, _ in _NO_KEY_CASES} == set(CONNECTORS.names())


@pytest.mark.asyncio
@pytest.mark.parametrize("connector_fn,kwargs", _NO_KEY_CASES, ids=[fn.name for fn, _ in _NO_KEY_CASES])
async def test_missing_key_raises_unauthorized(connector_fn, kwargs, monkeypatch) -> None:
    # Ensure no env fallback resolves a key.
    monkeypatch.delenv("ALPHA_VANTAGE_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        await connector_fn(**kwargs)
    assert exc_info.value.provider == "alpha_vantage"
    assert exc_info.value.env_var == "ALPHA_VANTAGE_API_KEY"


# ---------------------------------------------------------------------------
# §5.8 — HTTP-200 in-body error envelopes (the central correctness fix).
# Exercised through alpha_vantage_search; the helper is shared across all verbs.
# ---------------------------------------------------------------------------

# Verbatim Information body Alpha Vantage returns for BOTH rate-limit AND premium
# gates (captured live 2026-06-04) — byte-identical, hence both map to RateLimit.
_INFO_RATE_LIMIT = (
    "Thank you for using Alpha Vantage! Please consider spreading out your free API "
    "requests more sparingly (1 request per second). You may subscribe to any of the "
    "premium plans at https://www.alphavantage.co/premium/ to lift the free key rate "
    "limit (25 requests per day), raise the per-second burst limit, and instantly "
    "unlock all premium endpoints"
)


@respx.mock
@pytest.mark.asyncio
async def test_in_body_note_maps_rate_limit() -> None:
    _mock({"Note": "Thank you for using Alpha Vantage! Our standard API rate limit is 25/day..."})
    with pytest.raises(RateLimitError) as exc_info:
        await alpha_vantage_search.bind(api_key=_KEY)(keywords="x")
    assert exc_info.value.quota_exhausted is True
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_in_body_information_rate_limit_maps_rate_limit() -> None:
    # The real free-tier notice (rate-limit language) → RateLimitError.
    _mock({"Information": _INFO_RATE_LIMIT})
    with pytest.raises(RateLimitError) as exc_info:
        await alpha_vantage_search.bind(api_key=_KEY)(keywords="x")
    assert exc_info.value.quota_exhausted is True
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_in_body_information_premium_only_maps_payment_required() -> None:
    # A premium-gate notice WITHOUT rate-limit language → PaymentRequiredError.
    _mock({"Information": "This is a premium endpoint. Please subscribe to a premium plan."})
    with pytest.raises(PaymentRequiredError) as exc_info:
        await alpha_vantage_options.bind(api_key=_KEY)(symbol="IBM")
    assert exc_info.value.provider == "alpha_vantage"
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_in_body_error_message_maps_parse_error() -> None:
    _mock({"Error Message": "Invalid API call. Please retry or visit the documentation."})
    with pytest.raises(ParseError) as exc_info:
        await alpha_vantage_search.bind(api_key=_KEY)(keywords="x")
    assert exc_info.value.provider == "alpha_vantage"


@respx.mock
@pytest.mark.asyncio
async def test_csv_in_body_information_maps_rate_limit() -> None:
    # CSV endpoints return a JSON notice body (not CSV) on rate-limit.
    import json

    _mock(text=json.dumps({"Information": _INFO_RATE_LIMIT}))
    with pytest.raises(RateLimitError):
        await alpha_vantage_ipo_calendar.bind(api_key=_KEY)()


# ---------------------------------------------------------------------------
# Discovery / market data
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_search_returns_rows_and_strips_key() -> None:
    _mock(
        {
            "bestMatches": [
                {
                    "1. symbol": "AAPL",
                    "2. name": "Apple Inc",
                    "3. type": "Equity",
                    "4. region": "United States",
                    "8. currency": "USD",
                    "9. matchScore": "1.0000",
                }
            ]
        }
    )
    result = await alpha_vantage_search.bind(api_key=_KEY)(keywords="apple")

    assert result.provenance.source == "alpha_vantage_search"
    assert "api_key" not in result.provenance.params, "Theme-B: key leaked to provenance"
    assert _KEY not in str(result.provenance.params)
    df = result.data
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df.iloc[0]["name"] == "Apple Inc"


@respx.mock
@pytest.mark.asyncio
async def test_search_empty_matches_raises_empty_data() -> None:
    _mock({"bestMatches": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_search.bind(api_key=_KEY)(keywords="zzz")


@pytest.mark.asyncio
async def test_search_blank_keywords_raises_invalid_parameter() -> None:
    with pytest.raises(InvalidParameterError):
        await alpha_vantage_search.bind(api_key=_KEY)(keywords="   ")


@respx.mock
@pytest.mark.asyncio
async def test_quote_returns_single_row() -> None:
    _mock(
        {
            "Global Quote": {
                "01. symbol": "IBM",
                "05. price": "305.63",
                "02. open": "318.29",
                "03. high": "318.29",
                "04. low": "302.53",
                "06. volume": "13482372",
                "07. latest trading day": "2026-06-03",
                "08. previous close": "329.23",
                "09. change": "-23.60",
                "10. change percent": "-7.1682%",
            }
        }
    )
    result = await alpha_vantage_quote.bind(api_key=_KEY)(symbol="IBM")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "IBM"
    # `%` stripped → numeric coercion succeeds.
    assert df.iloc[0]["change_percent"] == pytest.approx(-7.1682)


@respx.mock
@pytest.mark.asyncio
async def test_quote_empty_raises_empty_data() -> None:
    _mock({"Global Quote": {}})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_quote.bind(api_key=_KEY)(symbol="NOPE")


def _ohlcv_body(ts_key: str) -> dict:
    return {
        "Meta Data": {"2. Symbol": "AAPL"},
        ts_key: {
            "2026-04-18": {
                "1. open": "170.00",
                "2. high": "172.00",
                "3. low": "169.00",
                "4. close": "171.50",
                "5. volume": "45000000",
            }
        },
    }


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fn,ts_key",
    [
        (alpha_vantage_daily, "Time Series (Daily)"),
        (alpha_vantage_weekly, "Weekly Time Series"),
        (alpha_vantage_monthly, "Monthly Time Series"),
    ],
    ids=["daily", "weekly", "monthly"],
)
async def test_ohlcv_series_inject_symbol_and_drop_meta(fn, ts_key) -> None:
    _mock(_ohlcv_body(ts_key))
    result = await fn.bind(api_key=_KEY)(symbol="AAPL")
    df = result.data
    # Symbol injected (raw payload has no symbol row field); Meta Data dropped.
    assert set(df.columns) == {"symbol", "date", "open", "high", "low", "close", "volume"}
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df["close"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_intraday_uses_timestamp_and_injects_symbol() -> None:
    _mock(
        {
            "Meta Data": {"2. Symbol": "AAPL"},
            "Time Series (60min)": {
                "2026-04-18 16:00:00": {
                    "1. open": "170.00",
                    "2. high": "172.00",
                    "3. low": "169.00",
                    "4. close": "171.50",
                    "5. volume": "100",
                }
            },
        }
    )
    result = await alpha_vantage_intraday.bind(api_key=_KEY)(symbol="AAPL", interval="60min")
    df = result.data
    assert "timestamp" in df.columns
    assert df.iloc[0]["symbol"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_daily_empty_raises_empty_data() -> None:
    _mock({"Meta Data": {}})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_daily.bind(api_key=_KEY)(symbol="IBM")


# ---------------------------------------------------------------------------
# Fundamentals (the formerly raw-dict loaders)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_overview_returns_single_keyed_row() -> None:
    _mock({"Symbol": "IBM", "Name": "International Business Machines", "Sector": "TECHNOLOGY", "PERatio": "None"})
    result = await alpha_vantage_overview.bind(api_key=_KEY)(symbol="IBM")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["Symbol"] == "IBM"
    assert df.iloc[0]["Name"] == "International Business Machines"
    # "None" sentinel cleaned to a real null.
    assert df.iloc[0]["PERatio"] is None


@respx.mock
@pytest.mark.asyncio
async def test_overview_empty_raises_empty_data() -> None:
    _mock({})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_overview.bind(api_key=_KEY)(symbol="IBM")


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fn,key",
    [
        (alpha_vantage_income_statement, "annualReports"),
        (alpha_vantage_balance_sheet, "annualReports"),
        (alpha_vantage_cash_flow, "annualReports"),
    ],
    ids=["income", "balance", "cashflow"],
)
async def test_statements_return_period_rows_keyed_by_symbol(fn, key) -> None:
    _mock({key: [{"fiscalDateEnding": "2025-12-31", "totalRevenue": "1000"}]})
    result = await fn.bind(api_key=_KEY)(symbol="IBM")
    df = result.data
    assert df.iloc[0]["symbol"] == "IBM"
    assert df.iloc[0]["fiscalDateEnding"] == "2025-12-31"


@respx.mock
@pytest.mark.asyncio
async def test_income_statement_empty_raises_empty_data() -> None:
    _mock({"annualReports": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_income_statement.bind(api_key=_KEY)(symbol="IBM")


@respx.mock
@pytest.mark.asyncio
async def test_earnings_returns_quarterly_rows() -> None:
    _mock(
        {
            "quarterlyEarnings": [
                {
                    "fiscalDateEnding": "2025-12-31",
                    "reportedEPS": "2.10",
                    "estimatedEPS": "2.00",
                    "surprise": "0.10",
                    "surprisePercentage": "5.0",
                    "reportTime": "post-market",
                }
            ]
        }
    )
    result = await alpha_vantage_earnings.bind(api_key=_KEY)(symbol="IBM")
    df = result.data
    assert df.iloc[0]["symbol"] == "IBM"
    assert df["reportedEPS"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_etf_profile_returns_holding_rows() -> None:
    _mock(
        {
            "net_assets": "500000000000",
            "holdings": [
                {"symbol": "AAPL", "description": "APPLE INC", "weight": "0.07"},
                {"symbol": "MSFT", "description": "MICROSOFT CORP", "weight": "0.06"},
            ],
        }
    )
    result = await alpha_vantage_etf_profile.bind(api_key=_KEY)(symbol="SPY")
    df = result.data
    assert df.iloc[0]["symbol"] == "SPY"
    assert set(df["holding_symbol"]) == {"AAPL", "MSFT"}
    assert df["weight"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_etf_profile_empty_raises_empty_data() -> None:
    _mock({"net_assets": "0", "holdings": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_etf_profile.bind(api_key=_KEY)(symbol="SPY")


# ---------------------------------------------------------------------------
# Forex (synthetic `pair` KEY injection)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fx_rate_returns_single_row() -> None:
    _mock(
        {
            "Realtime Currency Exchange Rate": {
                "1. From_Currency Code": "USD",
                "2. From_Currency Name": "United States Dollar",
                "3. To_Currency Code": "EUR",
                "4. To_Currency Name": "Euro",
                "5. Exchange Rate": "0.8611",
                "8. Bid Price": "0.8610",
                "9. Ask Price": "0.8612",
                "6. Last Refreshed": "2026-06-03 23:55:39",
            }
        }
    )
    result = await alpha_vantage_fx_rate.bind(api_key=_KEY)(from_currency="USD", to_currency="EUR")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["from_currency"] == "USD"
    assert df.iloc[0]["exchange_rate"] == pytest.approx(0.8611)


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fn,ts_key",
    [
        (alpha_vantage_fx_daily, "Time Series FX (Daily)"),
        (alpha_vantage_fx_weekly, "Time Series FX (Weekly)"),
        (alpha_vantage_fx_monthly, "Time Series FX (Monthly)"),
    ],
    ids=["fx_daily", "fx_weekly", "fx_monthly"],
)
async def test_fx_series_injects_pair_key(fn, ts_key) -> None:
    _mock(
        {
            ts_key: {
                "2026-04-18": {"1. open": "1.10", "2. high": "1.12", "3. low": "1.09", "4. close": "1.11"}
            }
        }
    )
    result = await fn.bind(api_key=_KEY)(from_symbol="EUR", to_symbol="USD")
    df = result.data
    # The synthetic KEY (no `pair` field in the raw payload).
    assert df.iloc[0]["pair"] == "EUR/USD"
    assert set(df.columns) == {"pair", "date", "open", "high", "low", "close"}


@respx.mock
@pytest.mark.asyncio
async def test_fx_daily_empty_raises_empty_data() -> None:
    _mock({"Time Series FX (Daily)": {}})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_fx_daily.bind(api_key=_KEY)(from_symbol="EUR", to_symbol="USD")


# ---------------------------------------------------------------------------
# Crypto (synthetic `symbol` KEY injection)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fn,ts_key",
    [
        (alpha_vantage_crypto_daily, "Time Series (Digital Currency Daily)"),
        (alpha_vantage_crypto_weekly, "Time Series (Digital Currency Weekly)"),
        (alpha_vantage_crypto_monthly, "Time Series (Digital Currency Monthly)"),
    ],
    ids=["crypto_daily", "crypto_weekly", "crypto_monthly"],
)
async def test_crypto_series_injects_symbol_key(fn, ts_key) -> None:
    _mock(
        {
            "Meta Data": {"2. Digital Currency Code": "BTC"},
            ts_key: {
                "2026-06-03": {
                    "1. open": "66658.45",
                    "2. high": "67027.64",
                    "3. low": "66555.59",
                    "4. close": "66790.14",
                    "5. volume": "753.69",
                }
            },
        }
    )
    result = await fn.bind(api_key=_KEY)(symbol="BTC", market="USD")
    df = result.data
    # Raw crypto rows carry NO symbol field — it must be injected from the param.
    assert df.iloc[0]["symbol"] == "BTC"
    assert set(df.columns) == {"symbol", "date", "open", "high", "low", "close", "volume"}
    assert df["close"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_crypto_daily_empty_raises_empty_data() -> None:
    _mock({"Time Series (Digital Currency Daily)": {}})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_crypto_daily.bind(api_key=_KEY)(symbol="BTC")


# ---------------------------------------------------------------------------
# Economic indicators / metals
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_econ_returns_observation_rows() -> None:
    _mock(
        {
            "name": "Real Gross Domestic Product",
            "unit": "billions of dollars",
            "interval": "quarterly",
            "data": [{"date": "2025-12-31", "value": "23000.0"}, {"date": "2025-09-30", "value": "."}],
        }
    )
    result = await alpha_vantage_econ.bind(api_key=_KEY)(function="REAL_GDP")
    df = result.data
    assert df.iloc[0]["name"] == "REAL_GDP"
    assert df["value"].notna().any()
    # "." sentinel coerces to null (not a crash).
    assert df["value"].isna().any()


@pytest.mark.asyncio
async def test_econ_bad_function_raises_invalid_parameter() -> None:
    with pytest.raises(InvalidParameterError):
        await alpha_vantage_econ.bind(api_key=_KEY)(function="NOT_A_REAL_INDICATOR")


@respx.mock
@pytest.mark.asyncio
async def test_econ_empty_raises_empty_data() -> None:
    _mock({"name": "Real GDP", "data": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_econ.bind(api_key=_KEY)(function="REAL_GDP")


@respx.mock
@pytest.mark.asyncio
async def test_metal_spot_returns_single_row() -> None:
    _mock({"nominal": "Gold", "price": "2350.50", "timestamp": "2026-06-03"})
    result = await alpha_vantage_metal_spot.bind(api_key=_KEY)(symbol="GOLD")
    df = result.data
    assert df.iloc[0]["symbol"] == "GOLD"
    assert df["price"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_metal_history_returns_price_rows() -> None:
    _mock({"data": [{"date": "2026-05-01", "price": "2340.0"}, {"date": "2026-04-01", "price": "."}]})
    result = await alpha_vantage_metal_history.bind(api_key=_KEY)(symbol="GOLD", interval="monthly")
    df = result.data
    assert df.iloc[0]["symbol"] == "GOLD"
    assert df["price"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_metal_spot_empty_raises_empty_data() -> None:
    _mock({"nominal": "Gold"})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_metal_spot.bind(api_key=_KEY)(symbol="GOLD")


# ---------------------------------------------------------------------------
# Alpha intelligence / options / technical
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_news_returns_article_rows() -> None:
    _mock(
        {
            "feed": [
                {
                    "title": "Markets rally",
                    "url": "https://example.com/a",
                    "time_published": "20260603T120000",
                    "source": "Wire",
                    "overall_sentiment_score": "0.3",
                    "overall_sentiment_label": "Bullish",
                    "summary": "Stocks up.",
                    "banner_image": "https://example.com/img.png",
                }
            ]
        }
    )
    result = await alpha_vantage_news.bind(api_key=_KEY)(tickers="AAPL", limit=5)
    df = result.data
    assert df.iloc[0]["title"] == "Markets rally"
    assert df["overall_sentiment_score"].notna().any()


@pytest.mark.asyncio
async def test_news_bad_limit_raises_invalid_parameter() -> None:
    with pytest.raises(InvalidParameterError):
        await alpha_vantage_news.bind(api_key=_KEY)(limit=0)


@respx.mock
@pytest.mark.asyncio
async def test_news_empty_raises_empty_data() -> None:
    _mock({"feed": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_news.bind(api_key=_KEY)()


@respx.mock
@pytest.mark.asyncio
async def test_top_movers_returns_categorized_rows() -> None:
    _mock(
        {
            "top_gainers": [
                {"ticker": "AAA", "price": "1.0", "change_amount": "0.5", "change_percentage": "50%", "volume": "100"}
            ],
            "top_losers": [
                {"ticker": "BBB", "price": "1.0", "change_amount": "-0.5", "change_percentage": "-33%", "volume": "200"}
            ],
            "most_actively_traded": [],
        }
    )
    result = await alpha_vantage_top_movers.bind(api_key=_KEY)()
    df = result.data
    assert set(df["category"]) == {"top_gainers", "top_losers"}
    assert df["change_percentage"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_top_movers_empty_raises_empty_data() -> None:
    _mock({"top_gainers": [], "top_losers": [], "most_actively_traded": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_top_movers.bind(api_key=_KEY)()


@respx.mock
@pytest.mark.asyncio
async def test_options_returns_contract_rows() -> None:
    _mock(
        {
            "data": [
                {
                    "contractID": "IBM260101C00100000",
                    "symbol": "IBM",
                    "expiration": "2026-01-01",
                    "strike": "100",
                    "type": "call",
                    "last": "5.0",
                    "bid": "4.9",
                    "ask": "5.1",
                    "volume": "10",
                    "open_interest": "100",
                    "implied_volatility": "0.3",
                    "delta": "0.5",
                    "gamma": "0.1",
                    "theta": "-0.02",
                    "vega": "0.2",
                }
            ]
        }
    )
    result = await alpha_vantage_options.bind(api_key=_KEY)(symbol="IBM")
    df = result.data
    assert df.iloc[0]["contractID"] == "IBM260101C00100000"
    assert df["strike"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_options_empty_raises_empty_data() -> None:
    _mock({"data": []})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_options.bind(api_key=_KEY)(symbol="IBM")


@respx.mock
@pytest.mark.asyncio
async def test_technical_injects_symbol_and_coerces_values() -> None:
    _mock(
        {
            "Meta Data": {"1: Symbol": "AAPL"},
            "Technical Analysis: SMA": {
                "2026-05-08": {"SMA": "200.34"},
                "2026-05-07": {"SMA": "200.21"},
            },
        }
    )
    result = await alpha_vantage_technical.bind(api_key=_KEY)(symbol="AAPL", function="SMA")
    df = result.data
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df["SMA"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_technical_intraday_preserves_time_component() -> None:
    """Regression: TECHNICAL_OUTPUT.date is `datetime` (not `date`) so intraday
    intervals keep their time component (date would `dt.normalize()` to midnight)."""
    _mock(
        {
            "Meta Data": {"1: Symbol": "AAPL", "4: Interval": "1min"},
            "Technical Analysis: SMA": {
                "2026-05-08 14:30:00": {"SMA": "200.34"},
                "2026-05-08 14:29:00": {"SMA": "200.21"},
            },
        }
    )
    result = await alpha_vantage_technical.bind(api_key=_KEY)(
        symbol="AAPL", function="SMA", interval="1min", time_period=20, series_type="close"
    )
    times = result.data["date"].dt.time.astype(str).tolist()
    assert "00:00:00" not in times, f"intraday timestamps normalized to midnight: {times}"
    assert set(times) == {"14:30:00", "14:29:00"}


@pytest.mark.asyncio
async def test_technical_bad_function_raises_invalid_parameter() -> None:
    with pytest.raises(InvalidParameterError):
        await alpha_vantage_technical.bind(api_key=_KEY)(symbol="AAPL", function="NOPE")


@respx.mock
@pytest.mark.asyncio
async def test_technical_empty_raises_empty_data() -> None:
    _mock({"Meta Data": {}})
    with pytest.raises(EmptyDataError):
        await alpha_vantage_technical.bind(api_key=_KEY)(symbol="AAPL", function="SMA")


# ---------------------------------------------------------------------------
# Calendars (CSV)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_earnings_calendar_returns_rows() -> None:
    csv = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency\n"
        "IBM,International Business Machines,2026-07-20,2026-06-30,2.50,USD\n"
    )
    _mock(text=csv)
    result = await alpha_vantage_earnings_calendar.bind(api_key=_KEY)(horizon="3month")
    df = result.data
    assert df.iloc[0]["symbol"] == "IBM"
    assert df["reportDate"].notna().any()


@respx.mock
@pytest.mark.asyncio
async def test_ipo_calendar_returns_rows() -> None:
    csv = (
        "symbol,name,ipoDate,priceRangeLow,priceRangeHigh,currency,exchange\n"
        "NEWCO,New Company,2026-07-01,18.0,20.0,USD,NASDAQ\n"
    )
    _mock(text=csv)
    result = await alpha_vantage_ipo_calendar.bind(api_key=_KEY)()
    df = result.data
    assert df.iloc[0]["symbol"] == "NEWCO"


@respx.mock
@pytest.mark.asyncio
async def test_ipo_calendar_empty_raises_empty_data() -> None:
    # Header-only CSV → empty frame.
    _mock(text="symbol,name,ipoDate,priceRangeLow,priceRangeHigh,currency,exchange\n")
    with pytest.raises(EmptyDataError):
        await alpha_vantage_ipo_calendar.bind(api_key=_KEY)()


# ---------------------------------------------------------------------------
# Enumerator (bounded)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_returns_declared_columns() -> None:
    csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
        "MSFT,Microsoft Corp,NASDAQ,Stock,1986-03-13,null,Active\n"
    )
    _mock(text=csv)
    result = await enumerate_alpha_vantage.bind(api_key=_KEY)(state="active")
    df = result.data
    # Enumerator drops unmapped (delistingDate) and exact-matches the schema.
    assert set(df.columns) == {"symbol", "name", "exchange", "assetType", "ipoDate", "status"}
    assert "AAPL" in set(df["symbol"])
    assert df["name"].str.len().gt(0).any()


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bounds_row_count() -> None:
    # A synthetic 6001-row CSV must be capped to the head slice.
    header = "symbol,name,exchange,assetType,ipoDate,status\n"
    body = "".join(f"SYM{i},Name {i},NASDAQ,Stock,2000-01-01,Active\n" for i in range(6001))
    _mock(text=header + body)
    result = await enumerate_alpha_vantage.bind(api_key=_KEY)(state="active")
    assert len(result.data) == 5000
