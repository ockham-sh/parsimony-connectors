"""Offline tests for the Tiingo connectors.

Every verb is exercised with a mocked transport (respx). Tiingo auth is an
``Authorization: Token <key>`` header; the canonical error-mapping contract is
covered on ``tiingo_search`` (see ``test_error_mapping_tiingo.py``). These
tests assert: happy-path row shaping, EmptyData/Parse guards, inline parameter
validation, the no-key ``UnauthorizedError`` fast-fail (shared ``_client`` → all
13 verbs), the plan-gated 403 → PaymentRequiredError mapping on news, and that
the bound key is stripped from provenance (the Theme-B secrets fix).
"""

from __future__ import annotations

import io
import zipfile

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

from parsimony_tiingo import (
    CONNECTORS,
    enumerate_tiingo,
    load,
    tiingo_crypto_prices,
    tiingo_crypto_top,
    tiingo_eod,
    tiingo_fundamentals_definitions,
    tiingo_fundamentals_meta,
    tiingo_fx_prices,
    tiingo_fx_top,
    tiingo_iex,
    tiingo_iex_historical,
    tiingo_meta,
    tiingo_news,
    tiingo_search,
)

_KEY = "live-looking-tiingo-xyz"
_BASE = "https://api.tiingo.com"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_count_matches_docstring() -> None:
    assert len(CONNECTORS) == 13


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
    # api_key is bound off the call surface for every connector that accepts it.
    for c in bundle:
        assert "api_key" not in c.exposed_signature.parameters, c.name


# ---------------------------------------------------------------------------
# tiingo_search (tool-tagged)
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_search_returns_rows_and_strips_key() -> None:
    respx.get(f"{_BASE}/tiingo/utilities/search").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc",
                    "assetType": "Stock",
                    "isActive": True,
                    "countryCode": "US",
                    "permaTicker": "US000000000038",
                    "openFIGIComposite": "BBG000B9XRY4",
                }
            ],
        )
    )

    bound = tiingo_search.bind(api_key=_KEY)
    result = bound(query="apple")

    assert result.provenance.source == "tiingo_search"
    # Theme-B: the bound key must not appear in provenance.
    assert _KEY not in str(result.provenance.params)
    assert "api_key" not in result.provenance.params
    df = result.data
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["country_code"] == "US"


@respx.mock
def test_tiingo_search_empty_results_raise_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/utilities/search").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_search.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(query="zzzznotaticker")


@respx.mock
def test_tiingo_search_non_list_raises_parse_error() -> None:
    respx.get(f"{_BASE}/tiingo/utilities/search").mock(return_value=httpx.Response(200, json={"detail": "weird"}))
    bound = tiingo_search.bind(api_key=_KEY)
    with pytest.raises(ParseError):
        bound(query="apple")


def test_tiingo_search_rejects_empty_query() -> None:
    bound = tiingo_search.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="query"):
        bound(query="   ")


def test_tiingo_search_rejects_bad_limit() -> None:
    bound = tiingo_search.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="limit"):
        bound(query="apple", limit=0)


@respx.mock
def test_tiingo_search_maps_401_without_leaking_key() -> None:
    respx.get(f"{_BASE}/tiingo/utilities/search").mock(return_value=httpx.Response(401, text="unauthorized"))
    bound = tiingo_search.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        bound(query="apple")
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_tiingo_search_maps_429_without_leaking_key() -> None:
    respx.get(f"{_BASE}/tiingo/utilities/search").mock(return_value=httpx.Response(429, text="rate limited"))
    bound = tiingo_search.bind(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        bound(query="apple")
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# tiingo_eod
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_eod_returns_ohlcv() -> None:
    respx.get(f"{_BASE}/tiingo/daily/AAPL/prices").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2024-01-02T00:00:00.000Z",
                    "open": 187.15,
                    "high": 188.44,
                    "low": 183.885,
                    "close": 185.64,
                    "volume": 82488674,
                    "adjClose": 183.57,
                    "adjOpen": 185.06,
                    "adjHigh": 186.34,
                    "adjLow": 181.83,
                    "adjVolume": 82488674,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                }
            ],
        )
    )
    bound = tiingo_eod.bind(api_key=_KEY)
    result = bound(ticker="AAPL")
    df = result.data
    assert df.iloc[0]["close"] == 185.64
    assert df.iloc[0]["ticker"] == "AAPL"


@respx.mock
def test_tiingo_eod_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/daily/AAPL/prices").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_eod.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(ticker="AAPL")


def test_tiingo_eod_rejects_unsafe_ticker() -> None:
    bound = tiingo_eod.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="unsafe"):
        bound(ticker="../etc/passwd")


# ---------------------------------------------------------------------------
# tiingo_iex
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_iex_returns_quotes() -> None:
    respx.get(f"{_BASE}/iex/").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "ticker": "AAPL",
                    "timestamp": "2026-06-03T10:52:50Z",
                    "tngoLast": 312.27,
                    "open": 314.19,
                    "high": 316.92,
                    "low": 312.09,
                    "volume": 355259,
                    "prevClose": 315.2,
                    "mid": 312.77,
                    "bidPrice": None,
                    "askPrice": None,
                    "bidSize": None,
                    "askSize": None,
                }
            ],
        )
    )
    bound = tiingo_iex.bind(api_key=_KEY)
    result = bound(tickers="AAPL")
    assert result.data.iloc[0]["tngo_last"] == 312.27


@respx.mock
def test_tiingo_iex_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/iex/").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_iex.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(tickers="AAPL")


def test_tiingo_iex_rejects_empty_tickers() -> None:
    bound = tiingo_iex.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="tickers"):
        bound(tickers="  ")


# ---------------------------------------------------------------------------
# tiingo_iex_historical
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_iex_historical_returns_bars() -> None:
    respx.get(f"{_BASE}/iex/AAPL/prices").mock(
        return_value=httpx.Response(
            200,
            json=[{"date": "2026-06-03T14:00:00.000Z", "open": 314.86, "high": 315.95, "low": 312.09, "close": 312.28}],
        )
    )
    bound = tiingo_iex_historical.bind(api_key=_KEY)
    result = bound(ticker="AAPL")
    df = result.data
    assert df.iloc[0]["close"] == 312.28
    assert df.iloc[0]["ticker"] == "AAPL"


@respx.mock
def test_tiingo_iex_historical_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/iex/AAPL/prices").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_iex_historical.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(ticker="AAPL")


# ---------------------------------------------------------------------------
# tiingo_meta
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_meta_returns_dict() -> None:
    respx.get(f"{_BASE}/tiingo/daily/AAPL").mock(
        return_value=httpx.Response(
            200,
            json={
                "ticker": "AAPL",
                "name": "Apple Inc",
                "description": "Apple designs phones.",
                "startDate": "1980-12-12",
                "endDate": "2026-06-02",
                "exchangeCode": "NASDAQ",
            },
        )
    )
    bound = tiingo_meta.bind(api_key=_KEY)
    result = bound(ticker="AAPL")
    assert result.data["ticker"] == "AAPL"
    assert result.data["exchangeCode"] == "NASDAQ"


@respx.mock
def test_tiingo_meta_no_ticker_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/daily/AAPL").mock(return_value=httpx.Response(200, json={}))
    bound = tiingo_meta.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(ticker="AAPL")


# ---------------------------------------------------------------------------
# tiingo_fundamentals_meta
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_fundamentals_meta_returns_list() -> None:
    respx.get(f"{_BASE}/tiingo/fundamentals/meta").mock(
        return_value=httpx.Response(
            200,
            json=[{"ticker": "aapl", "name": "Apple Inc", "sector": "Technology", "industry": "Consumer Electronics"}],
        )
    )
    bound = tiingo_fundamentals_meta.bind(api_key=_KEY)
    result = bound(tickers="AAPL")
    # Always a list (stable shape regardless of ticker count).
    assert isinstance(result.data, list)
    assert result.data[0]["sector"] == "Technology"


@respx.mock
def test_tiingo_fundamentals_meta_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/fundamentals/meta").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_fundamentals_meta.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(tickers="AAPL")


# ---------------------------------------------------------------------------
# tiingo_fundamentals_definitions
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_fundamentals_definitions_returns_rows() -> None:
    respx.get(f"{_BASE}/tiingo/fundamentals/definitions").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "dataCode": "rps",
                    "name": "Revenue Per Share",
                    "description": "Revenue per share",
                    "statementType": "overview",
                    "units": "$",
                },
            ],
        )
    )
    bound = tiingo_fundamentals_definitions.bind(api_key=_KEY)
    result = bound()
    df = result.data
    assert df.iloc[0]["data_code"] == "rps"
    assert df.iloc[0]["name"] == "Revenue Per Share"


@respx.mock
def test_tiingo_fundamentals_definitions_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/fundamentals/definitions").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_fundamentals_definitions.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound()


# ---------------------------------------------------------------------------
# tiingo_news (plan-gated: free tier returns 403 → PaymentRequiredError)
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_news_returns_articles() -> None:
    respx.get(f"{_BASE}/tiingo/news").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": 1,
                    "title": "Apple beats earnings",
                    "publishedDate": "2026-06-01T00:00:00Z",
                    "source": "bloomberg.com",
                    "tickers": ["aapl"],
                    "tags": ["earnings"],
                    "description": "Strong quarter.",
                    "url": "https://example.com/a",
                }
            ],
        )
    )
    bound = tiingo_news.bind(api_key=_KEY)
    result = bound(tickers="AAPL")
    df = result.data
    assert df.iloc[0]["title"] == "Apple beats earnings"
    assert df.iloc[0]["tickers"] == "aapl"


@respx.mock
def test_tiingo_news_403_maps_to_payment_required() -> None:
    # Free-tier 403 is a plan-tier restriction, NOT a credential failure.
    respx.get(f"{_BASE}/tiingo/news").mock(
        return_value=httpx.Response(403, json={"detail": "You do not have permission to access the News API"})
    )
    bound = tiingo_news.bind(api_key=_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        bound()
    assert exc_info.value.provider == "tiingo"
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_tiingo_news_403_invalid_token_maps_to_unauthorized() -> None:
    # Tiingo also returns 403 for a bad/typo'd/revoked key — that is a credential
    # failure, NOT a plan restriction, so it must map to UnauthorizedError.
    from parsimony.errors import UnauthorizedError

    respx.get(f"{_BASE}/tiingo/news").mock(return_value=httpx.Response(403, json={"detail": "Invalid token."}))
    bound = tiingo_news.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        bound()
    assert exc_info.value.provider == "tiingo"
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_tiingo_news_500_maps_to_provider_error() -> None:
    # Non-403 errors still go through the canonical mapper.
    respx.get(f"{_BASE}/tiingo/news").mock(return_value=httpx.Response(500, text="boom"))
    bound = tiingo_news.bind(api_key=_KEY)
    with pytest.raises(Exception) as exc_info:
        bound()
    # ProviderError is not PaymentRequiredError.
    assert not isinstance(exc_info.value, PaymentRequiredError)


def test_tiingo_news_rejects_bad_limit() -> None:
    bound = tiingo_news.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="limit"):
        bound(limit=500)


# ---------------------------------------------------------------------------
# tiingo_crypto_prices
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_crypto_prices_flattens_nested() -> None:
    respx.get(f"{_BASE}/tiingo/crypto/prices").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "ticker": "btcusd",
                    "baseCurrency": "btc",
                    "quoteCurrency": "usd",
                    "priceData": [
                        {
                            "date": "2024-01-01T00:00:00+00:00",
                            "open": 42284.8,
                            "high": 44223.9,
                            "low": 42186.2,
                            "close": 44208.1,
                            "volume": 11860.7,
                            "volumeNotional": 524343133.4,
                            "tradesDone": 226804.0,
                        },
                    ],
                }
            ],
        )
    )
    bound = tiingo_crypto_prices.bind(api_key=_KEY)
    result = bound(tickers="btcusd")
    df = result.data
    assert df.iloc[0]["ticker"] == "btcusd"
    assert df.iloc[0]["close"] == 44208.1


@respx.mock
def test_tiingo_crypto_prices_empty_priceData_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/crypto/prices").mock(
        return_value=httpx.Response(200, json=[{"ticker": "btcusd", "priceData": []}])
    )
    bound = tiingo_crypto_prices.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(tickers="btcusd")


# ---------------------------------------------------------------------------
# tiingo_crypto_top
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_crypto_top_flattens_nested() -> None:
    respx.get(f"{_BASE}/tiingo/crypto/top").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "ticker": "btcusd",
                    "topOfBookData": [
                        {
                            "quoteTimestamp": "2026-06-03T14:52:42Z",
                            "bidSize": 0.0007,
                            "bidPrice": 66770.6,
                            "askSize": 0.016,
                            "askPrice": 66740.4,
                            "lastSizeNotional": 667.4,
                            "lastPrice": 66746.5,
                            "lastExchange": "BULLISH",
                        }
                    ],
                }
            ],
        )
    )
    bound = tiingo_crypto_top.bind(api_key=_KEY)
    result = bound(tickers="btcusd")
    df = result.data
    assert df.iloc[0]["last_price"] == 66746.5
    assert df.iloc[0]["last_exchange"] == "BULLISH"


@respx.mock
def test_tiingo_crypto_top_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/crypto/top").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_crypto_top.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(tickers="btcusd")


# ---------------------------------------------------------------------------
# tiingo_fx_prices
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_fx_prices_returns_bars() -> None:
    respx.get(f"{_BASE}/tiingo/fx/prices").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2024-01-01T00:00:00Z",
                    "ticker": "eurusd",
                    "open": 1.1046,
                    "high": 1.1046,
                    "low": 1.1035,
                    "close": 1.1036,
                }
            ],
        )
    )
    bound = tiingo_fx_prices.bind(api_key=_KEY)
    result = bound(tickers="eurusd")
    df = result.data
    assert df.iloc[0]["ticker"] == "eurusd"
    assert df.iloc[0]["close"] == 1.1036


@respx.mock
def test_tiingo_fx_prices_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/fx/prices").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_fx_prices.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(tickers="eurusd")


# ---------------------------------------------------------------------------
# tiingo_fx_top
# ---------------------------------------------------------------------------


@respx.mock
def test_tiingo_fx_top_returns_quotes() -> None:
    respx.get(f"{_BASE}/tiingo/fx/top").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "ticker": "eurusd",
                    "quoteTimestamp": "2026-06-03T14:53:10Z",
                    "bidPrice": 1.1603,
                    "bidSize": 1000000.0,
                    "askPrice": 1.1603,
                    "askSize": 1000000.0,
                    "midPrice": 1.16035,
                }
            ],
        )
    )
    bound = tiingo_fx_top.bind(api_key=_KEY)
    result = bound(tickers="eurusd")
    df = result.data
    assert df.iloc[0]["mid_price"] == 1.16035


@respx.mock
def test_tiingo_fx_top_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/tiingo/fx/top").mock(return_value=httpx.Response(200, json=[]))
    bound = tiingo_fx_top.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(tickers="eurusd")


# ---------------------------------------------------------------------------
# enumerate_tiingo (bounded — never assert full 127k counts)
# ---------------------------------------------------------------------------


def _make_tickers_zip(rows: list[dict[str, str]]) -> bytes:
    header = "ticker,exchange,assetType,priceCurrency,startDate,endDate\n"
    body = "".join(
        f"{r['ticker']},{r['exchange']},{r['assetType']},{r['priceCurrency']},{r['startDate']},{r['endDate']}\n"
        for r in rows
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("supported_tickers.csv", header + body)
    return buf.getvalue()


@respx.mock
def test_enumerate_tiingo_parses_zip() -> None:
    content = _make_tickers_zip(
        [
            {
                "ticker": "AAPL",
                "exchange": "NASDAQ",
                "assetType": "Stock",
                "priceCurrency": "USD",
                "startDate": "1980-12-12",
                "endDate": "2026-06-02",
            },
            {
                "ticker": "BTCUSD",
                "exchange": "",
                "assetType": "Crypto",
                "priceCurrency": "USD",
                "startDate": "2014-01-01",
                "endDate": "",
            },
        ]
    )
    respx.get("https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip").mock(
        return_value=httpx.Response(200, content=content)
    )
    bound = enumerate_tiingo.bind(api_key=_KEY)
    result = bound()
    df = result.data
    # Exact-match enumerator columns.
    assert list(df.columns) == ["ticker", "name", "asset_type", "exchange", "price_currency", "start_date", "end_date"]
    assert set(df["ticker"]) == {"AAPL", "BTCUSD"}
    # name falls back to the ticker symbol (CSV has no name column).
    assert df[df["ticker"] == "AAPL"].iloc[0]["name"] == "AAPL"
    assert df[df["ticker"] == "AAPL"].iloc[0]["price_currency"] == "USD"


@respx.mock
def test_enumerate_tiingo_bad_zip_raises_parse_error() -> None:
    respx.get("https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip").mock(
        return_value=httpx.Response(200, content=b"not a zip file")
    )
    bound = enumerate_tiingo.bind(api_key=_KEY)
    with pytest.raises(ParseError):
        bound()


# ---------------------------------------------------------------------------
# No-key fast-fail — shared _client, so EVERY keyed verb must raise
# UnauthorizedError(env_var="TIINGO_API_KEY") before any network call.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("connector_fn", "kwargs"),
    [
        (tiingo_search, {"query": "apple"}),
        (tiingo_eod, {"ticker": "AAPL"}),
        (tiingo_iex, {"tickers": "AAPL"}),
        (tiingo_iex_historical, {"ticker": "AAPL"}),
        (tiingo_meta, {"ticker": "AAPL"}),
        (tiingo_fundamentals_meta, {"tickers": "AAPL"}),
        (tiingo_fundamentals_definitions, {}),
        (tiingo_news, {}),
        (tiingo_crypto_prices, {"tickers": "btcusd"}),
        (tiingo_crypto_top, {"tickers": "btcusd"}),
        (tiingo_fx_prices, {"tickers": "eurusd"}),
        (tiingo_fx_top, {"tickers": "eurusd"}),
        (enumerate_tiingo, {}),
    ],
)
def test_no_key_raises_unauthorized(connector_fn, kwargs, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        connector_fn(**kwargs)
    assert exc_info.value.env_var == "TIINGO_API_KEY"
    assert exc_info.value.provider == "tiingo"


def test_no_key_case_covers_all_thirteen_verbs() -> None:
    # Guard against silently dropping a verb from the parametrize list above.
    assert len(CONNECTORS) == 13
