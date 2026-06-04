"""Offline (respx-mocked) tests for the Finnhub connectors.

Covers every verb's happy path, the EmptyData / ParseError / InvalidParameter
guards, and the no-key ``UnauthorizedError`` fast-fail. Live behaviour is
verified separately in ``test_integration_finnhub.py``.

Finnhub auth is the ``token`` query parameter; the no-key path raises
``UnauthorizedError`` before any network call (so several tests use no respx
route at all).
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
    UnauthorizedError,
)

from parsimony_finnhub import (
    CONNECTORS,
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
    load,
)

_KEY = "live-looking-finnhub-key"
_BASE = "https://finnhub.io/api/v1"

# Every keyed verb and a representative call. Used to assert the symmetric
# no-key fast-fail across the whole package.
_ALL_KEYED = [
    (finnhub_search, {"query": "apple"}),
    (finnhub_quote, {"symbol": "AAPL"}),
    (finnhub_profile, {"symbol": "AAPL"}),
    (finnhub_peers, {"symbol": "AAPL"}),
    (finnhub_recommendation, {"symbol": "AAPL"}),
    (finnhub_earnings, {"symbol": "AAPL"}),
    (finnhub_basic_financials, {"symbol": "AAPL"}),
    (finnhub_company_news, {"symbol": "AAPL", "from_date": "2024-01-01", "to_date": "2024-01-31"}),
    (finnhub_market_news, {}),
    (finnhub_earnings_calendar, {"from_date": "2024-01-01", "to_date": "2024-01-31"}),
    (finnhub_ipo_calendar, {"from_date": "2024-01-01", "to_date": "2024-01-31"}),
    (enumerate_finnhub, {}),
]


# ---------------------------------------------------------------------------
# Package surface
# ---------------------------------------------------------------------------


def test_connectors_count() -> None:
    assert len(CONNECTORS) == 12


def test_every_verb_declares_api_key_secret() -> None:
    """Theme B: the bound api_key must be stripped from provenance on EVERY verb."""
    for c in CONNECTORS:
        assert "api_key" in c.secrets, f"{c.name} is missing secrets=('api_key',)"


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


def test_load_binds_key_off_call_surface() -> None:
    bound = load(api_key=_KEY)
    sig = bound["finnhub_quote"].exposed_signature
    assert "api_key" not in sig.parameters


# ---------------------------------------------------------------------------
# No-key fast-fail (symmetric across all keyed verbs, incl. the enumerator)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("connector,kwargs", _ALL_KEYED, ids=[c.name for c, _ in _ALL_KEYED])
async def test_no_key_raises_unauthorized_with_env_var(connector, kwargs, monkeypatch) -> None:
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        await connector(**kwargs)
    assert exc_info.value.env_var == "FINNHUB_API_KEY"
    assert exc_info.value.provider == "finnhub"


@pytest.mark.asyncio
async def test_env_var_fallback_resolves_key(monkeypatch) -> None:
    """A key in the env (no bind / call-time arg) is picked up by _client."""
    monkeypatch.setenv("FINNHUB_API_KEY", _KEY)
    with respx.mock:
        respx.get(f"{_BASE}/quote").mock(
            return_value=httpx.Response(200, json={"c": 171.5, "pc": 170.5})
        )
        result = await finnhub_quote(symbol="AAPL")
    assert result.data.iloc[0]["symbol"] == "AAPL"


# ---------------------------------------------------------------------------
# Discovery — search
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_search_returns_matches() -> None:
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "result": [
                    {
                        "description": "APPLE INC",
                        "displaySymbol": "AAPL",
                        "symbol": "AAPL",
                        "type": "Common Stock",
                    }
                ],
            },
        )
    )
    result = await finnhub_search.bind(api_key=_KEY)(query="apple")
    assert result.provenance.source == "finnhub_search"
    assert result.data.iloc[0]["symbol"] == "AAPL"
    assert result.data.iloc[0]["description"] == "APPLE INC"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_search_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/search").mock(return_value=httpx.Response(200, json={"count": 0, "result": []}))
    with pytest.raises(EmptyDataError):
        await finnhub_search.bind(api_key=_KEY)(query="zzznotreal")


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_search_non_object_raises_parse() -> None:
    respx.get(f"{_BASE}/search").mock(return_value=httpx.Response(200, json=[1, 2, 3]))
    with pytest.raises(ParseError):
        await finnhub_search.bind(api_key=_KEY)(query="apple")


@pytest.mark.asyncio
async def test_finnhub_search_blank_query_raises_invalid_param() -> None:
    with pytest.raises(InvalidParameterError):
        await finnhub_search.bind(api_key=_KEY)(query="   ")


@respx.mock
@pytest.mark.asyncio
async def test_search_provenance_excludes_api_key() -> None:
    """api_key passed at CALL TIME must not land in provenance (Theme B)."""
    respx.get(f"{_BASE}/search").mock(
        return_value=httpx.Response(200, json={"result": [{"symbol": "AAPL"}]})
    )
    # Call with api_key as a keyword arg, NOT bound — secrets= must still strip it.
    result = await finnhub_search(query="apple", api_key=_KEY)
    assert "api_key" not in result.provenance.params
    assert _KEY not in str(result.provenance.params)


# ---------------------------------------------------------------------------
# Quote
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_quote_returns_single_row() -> None:
    respx.get(f"{_BASE}/quote").mock(
        return_value=httpx.Response(
            200,
            json={"c": 171.5, "d": 1.0, "dp": 0.6, "h": 172.0, "l": 169.0, "o": 170.0, "pc": 170.5, "t": 1_700_000_000},
        )
    )
    result = await finnhub_quote.bind(api_key=_KEY)(symbol="AAPL")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df.iloc[0]["current_price"] == 171.5


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_quote_no_price_raises_empty_data() -> None:
    respx.get(f"{_BASE}/quote").mock(return_value=httpx.Response(200, json={"c": None}))
    with pytest.raises(EmptyDataError):
        await finnhub_quote.bind(api_key=_KEY)(symbol="ZZZZ")


# ---------------------------------------------------------------------------
# Profile (dict return)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_profile_returns_dict() -> None:
    respx.get(f"{_BASE}/stock/profile2").mock(
        return_value=httpx.Response(200, json={"name": "Apple Inc", "ticker": "AAPL", "country": "US"})
    )
    result = await finnhub_profile.bind(api_key=_KEY)(symbol="AAPL")
    assert result.data["name"] == "Apple Inc"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_profile_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/stock/profile2").mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(EmptyDataError):
        await finnhub_profile.bind(api_key=_KEY)(symbol="ZZZZ")


# ---------------------------------------------------------------------------
# Peers
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_peers_returns_symbols() -> None:
    respx.get(f"{_BASE}/stock/peers").mock(return_value=httpx.Response(200, json=["AAPL", "DELL", "HPQ"]))
    result = await finnhub_peers.bind(api_key=_KEY)(symbol="AAPL")
    assert set(result.data["symbol"]) == {"AAPL", "DELL", "HPQ"}


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_peers_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/stock/peers").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        await finnhub_peers.bind(api_key=_KEY)(symbol="ZZZZ")


# ---------------------------------------------------------------------------
# Recommendation
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_recommendation_returns_rows() -> None:
    respx.get(f"{_BASE}/stock/recommendation").mock(
        return_value=httpx.Response(
            200,
            json=[{"period": "2026-06-01", "strongBuy": 14, "buy": 24, "hold": 15, "sell": 2, "strongSell": 0}],
        )
    )
    result = await finnhub_recommendation.bind(api_key=_KEY)(symbol="AAPL")
    df = result.data
    assert df.iloc[0]["strong_buy"] == 14
    assert df.iloc[0]["buy"] == 24


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_recommendation_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/stock/recommendation").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        await finnhub_recommendation.bind(api_key=_KEY)(symbol="ZZZZ")


# ---------------------------------------------------------------------------
# Earnings
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_earnings_returns_rows() -> None:
    respx.get(f"{_BASE}/stock/earnings").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"period": "2026-03-31", "quarter": 2, "year": 2026, "actual": 2.01,
                 "estimate": 1.9884, "surprise": 0.0216, "surprisePercent": 1.0863}
            ],
        )
    )
    result = await finnhub_earnings.bind(api_key=_KEY)(symbol="AAPL")
    df = result.data
    assert df.iloc[0]["eps_actual"] == 2.01
    assert df.iloc[0]["eps_estimate"] == 1.9884


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_earnings_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/stock/earnings").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        await finnhub_earnings.bind(api_key=_KEY)(symbol="ZZZZ")


# ---------------------------------------------------------------------------
# Basic financials (dict return)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_basic_financials_returns_dict() -> None:
    respx.get(f"{_BASE}/stock/metric").mock(
        return_value=httpx.Response(200, json={"metric": {"peTTM": 30.5}, "series": {"annual": {}}})
    )
    result = await finnhub_basic_financials.bind(api_key=_KEY)(symbol="AAPL")
    assert result.data["metric"]["peTTM"] == 30.5


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_basic_financials_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/stock/metric").mock(return_value=httpx.Response(200, json={"metric": {}}))
    with pytest.raises(EmptyDataError):
        await finnhub_basic_financials.bind(api_key=_KEY)(symbol="ZZZZ")


# ---------------------------------------------------------------------------
# Company news
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_company_news_returns_rows() -> None:
    respx.get(f"{_BASE}/company-news").mock(
        return_value=httpx.Response(
            200,
            json=[{"id": 1, "datetime": 1_700_000_000, "headline": "Apple beats", "source": "Reuters",
                   "category": "company", "related": "AAPL", "summary": "...", "url": "http://x", "image": ""}],
        )
    )
    result = await finnhub_company_news.bind(api_key=_KEY)(
        symbol="AAPL", from_date="2024-01-01", to_date="2024-01-31"
    )
    assert result.data.iloc[0]["headline"] == "Apple beats"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_company_news_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/company-news").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        await finnhub_company_news.bind(api_key=_KEY)(
            symbol="AAPL", from_date="1990-01-01", to_date="1990-01-31"
        )


@pytest.mark.asyncio
async def test_finnhub_company_news_bad_date_order_raises_invalid_param() -> None:
    with pytest.raises(InvalidParameterError):
        await finnhub_company_news.bind(api_key=_KEY)(
            symbol="AAPL", from_date="2024-02-01", to_date="2024-01-01"
        )


# ---------------------------------------------------------------------------
# Market news
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_market_news_returns_rows() -> None:
    respx.get(f"{_BASE}/news").mock(
        return_value=httpx.Response(
            200,
            json=[{"id": 9, "datetime": 1_700_000_000, "headline": "Markets up", "source": "AP",
                   "category": "business", "related": "", "summary": "...", "url": "http://x", "image": ""}],
        )
    )
    result = await finnhub_market_news.bind(api_key=_KEY)(category="general")
    assert result.data.iloc[0]["headline"] == "Markets up"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_market_news_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/news").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        await finnhub_market_news.bind(api_key=_KEY)(category="crypto")


# ---------------------------------------------------------------------------
# Earnings calendar
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_earnings_calendar_returns_rows() -> None:
    respx.get(f"{_BASE}/calendar/earnings").mock(
        return_value=httpx.Response(
            200,
            json={"earningsCalendar": [
                {"symbol": "WSE", "date": "2026-06-30", "year": 2026, "quarter": 4, "hour": "",
                 "epsEstimate": 0.1949, "epsActual": None, "revenueEstimate": 881805750, "revenueActual": None}
            ]},
        )
    )
    result = await finnhub_earnings_calendar.bind(api_key=_KEY)(from_date="2026-06-01", to_date="2026-06-30")
    assert result.data.iloc[0]["symbol"] == "WSE"
    assert result.data.iloc[0]["eps_estimate"] == 0.1949


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_earnings_calendar_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/calendar/earnings").mock(
        return_value=httpx.Response(200, json={"earningsCalendar": []})
    )
    with pytest.raises(EmptyDataError):
        await finnhub_earnings_calendar.bind(api_key=_KEY)(from_date="1990-01-01", to_date="1990-01-31")


@pytest.mark.asyncio
async def test_finnhub_earnings_calendar_bad_date_order_raises_invalid_param() -> None:
    with pytest.raises(InvalidParameterError):
        await finnhub_earnings_calendar.bind(api_key=_KEY)(from_date="2024-02-01", to_date="2024-01-01")


# ---------------------------------------------------------------------------
# IPO calendar — price_range preserves the verbatim string (single + range)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_ipo_calendar_preserves_price_range_string() -> None:
    respx.get(f"{_BASE}/calendar/ipo").mock(
        return_value=httpx.Response(
            200,
            json={"ipoCalendar": [
                {"symbol": "FRBT", "name": "Forbright, Inc.", "date": "2026-06-11",
                 "exchange": "NASDAQ", "status": "expected", "price": "18.00-20.00",
                 "numberOfShares": 7900000, "totalSharesValue": 181700000},
                {"symbol": "X", "name": "Single Co", "date": "2026-06-10",
                 "exchange": "NYSE", "status": "priced", "price": "10.00",
                 "numberOfShares": 100, "totalSharesValue": 1000},
            ]},
        )
    )
    result = await finnhub_ipo_calendar.bind(api_key=_KEY)(from_date="2026-06-01", to_date="2026-06-30")
    df = result.data
    # Range string is preserved verbatim (the old float() parse silently nulled it).
    prices = dict(zip(df["name"], df["price_range"], strict=True))
    assert prices["Forbright, Inc."] == "18.00-20.00"
    assert prices["Single Co"] == "10.00"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_ipo_calendar_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/calendar/ipo").mock(return_value=httpx.Response(200, json={"ipoCalendar": []}))
    with pytest.raises(EmptyDataError):
        await finnhub_ipo_calendar.bind(api_key=_KEY)(from_date="1990-01-01", to_date="1990-01-31")


# ---------------------------------------------------------------------------
# Enumerator — now routes through finnhub_get (error mapping) + exact columns
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_finnhub_returns_exact_columns() -> None:
    respx.get(f"{_BASE}/stock/symbol").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"symbol": "AAPL", "description": "APPLE INC", "displaySymbol": "AAPL",
                 "type": "Common Stock", "currency": "USD", "mic": "XNAS", "isin": "US0378331005"},
            ],
        )
    )
    result = await enumerate_finnhub.bind(api_key=_KEY)(exchange="US")
    df = result.data
    assert list(df.columns) == [
        "symbol", "description", "display_symbol", "type", "currency", "mic", "exchange", "isin"
    ]
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df.iloc[0]["exchange"] == "US"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_finnhub_premium_403_maps_to_payment_required() -> None:
    """The enumerator path now has error mapping (it had none before)."""
    respx.get(f"{_BASE}/stock/symbol").mock(return_value=httpx.Response(403, text="no access"))
    with pytest.raises(PaymentRequiredError):
        await enumerate_finnhub.bind(api_key=_KEY)(exchange="US")


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_finnhub_empty_raises_empty_data() -> None:
    respx.get(f"{_BASE}/stock/symbol").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        await enumerate_finnhub.bind(api_key=_KEY)(exchange="US")
