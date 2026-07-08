"""Offline unit tests for the Polymarket connectors (mocked HTTP)."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, ProviderError

from parsimony_polymarket import (
    CONNECTORS,
    polymarket_events,
    polymarket_market_prices,
    polymarket_markets,
)

_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
_EVENTS_URL = "https://gamma-api.polymarket.com/events"
_PRICE_URL = "https://clob.polymarket.com/price"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"polymarket_markets", "polymarket_events", "polymarket_market_prices"}


def test_markets_and_events_are_enumerators() -> None:
    by_name = {c.name: c for c in CONNECTORS}
    assert "enumerator" in by_name["polymarket_markets"].tags
    assert "enumerator" in by_name["polymarket_events"].tags
    # market_prices is a scalar lookup, not entity discovery → plain connector.
    assert "enumerator" not in by_name["polymarket_market_prices"].tags


# ---------------------------------------------------------------------------
# polymarket_markets
# ---------------------------------------------------------------------------


@respx.mock
def test_polymarket_markets_returns_declared_columns() -> None:
    respx.get(_MARKETS_URL).mock(
        return_value=httpx.Response(
            200,
            json=[
                # Extra junk fields must be dropped by the enumerator exact-match.
                {
                    "id": "1",
                    "question": "Will X happen?",
                    "slug": "x",
                    "active": True,
                    "clobTokenIds": '["111", "222"]',
                    "volume": "5",
                },
            ],
        )
    )
    result = polymarket_markets(limit=5)
    assert result.provenance.source == "polymarket_markets"
    assert list(result.data.columns) == ["id", "question", "slug", "active", "clobTokenIds"]
    assert result.data.iloc[0]["slug"] == "x"
    assert result.data.iloc[0]["clobTokenIds"] == '["111", "222"]'


@respx.mock
def test_polymarket_markets_raises_empty_data_when_no_rows() -> None:
    respx.get(_MARKETS_URL).mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        polymarket_markets(limit=5)


@respx.mock
def test_polymarket_markets_raises_parse_error_on_non_list() -> None:
    respx.get(_MARKETS_URL).mock(return_value=httpx.Response(200, json={"error": "nope"}))
    with pytest.raises(ParseError):
        polymarket_markets(limit=5)


@respx.mock
def test_polymarket_markets_raises_parse_error_on_missing_fields() -> None:
    # 200 with a list, but rows lack the declared TITLE/KEY fields.
    respx.get(_MARKETS_URL).mock(return_value=httpx.Response(200, json=[{"foo": "bar"}]))
    with pytest.raises(ParseError):
        polymarket_markets(limit=5)


@respx.mock
def test_polymarket_markets_maps_500() -> None:
    respx.get(_MARKETS_URL).mock(return_value=httpx.Response(500, text="err"))
    with pytest.raises(ProviderError):
        polymarket_markets(limit=5)


def test_polymarket_markets_rejects_out_of_range_limit() -> None:
    with pytest.raises(InvalidParameterError):
        polymarket_markets(limit=0)
    with pytest.raises(InvalidParameterError):
        polymarket_markets(limit=101)


# ---------------------------------------------------------------------------
# polymarket_events
# ---------------------------------------------------------------------------


@respx.mock
def test_polymarket_events_returns_declared_columns() -> None:
    respx.get(_EVENTS_URL).mock(
        return_value=httpx.Response(
            200,
            json=[{"id": "e1", "title": "Election", "slug": "election", "noise": 1}],
        )
    )
    result = polymarket_events(limit=5)
    assert result.provenance.source == "polymarket_events"
    assert list(result.data.columns) == ["id", "title", "slug"]


@respx.mock
def test_polymarket_events_raises_empty_data_when_no_rows() -> None:
    respx.get(_EVENTS_URL).mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        polymarket_events(limit=5)


@respx.mock
def test_polymarket_events_raises_parse_error_on_missing_fields() -> None:
    respx.get(_EVENTS_URL).mock(return_value=httpx.Response(200, json=[{"id": "e1"}]))
    with pytest.raises(ParseError):
        polymarket_events(limit=5)


def test_polymarket_events_rejects_out_of_range_limit() -> None:
    with pytest.raises(InvalidParameterError):
        polymarket_events(limit=0)


# ---------------------------------------------------------------------------
# polymarket_market_prices
# ---------------------------------------------------------------------------


@respx.mock
def test_polymarket_market_prices_returns_dict() -> None:
    respx.get(_PRICE_URL).mock(return_value=httpx.Response(200, json={"price": "0.42"}))
    result = polymarket_market_prices(token_id="abc")
    # Scalar/dict return → Result (no .df); the price is coerced str→float.
    assert result.data["price"] == 0.42
    assert isinstance(result.data["price"], float)


@respx.mock
def test_polymarket_market_prices_raises_parse_error_on_non_numeric() -> None:
    respx.get(_PRICE_URL).mock(return_value=httpx.Response(200, json={"price": "n/a"}))
    with pytest.raises(ParseError):
        polymarket_market_prices(token_id="abc")


@respx.mock
def test_polymarket_market_prices_raises_empty_data_when_no_price() -> None:
    # 200 but the body has no "price" key.
    respx.get(_PRICE_URL).mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(EmptyDataError):
        polymarket_market_prices(token_id="abc")


@respx.mock
def test_polymarket_market_prices_raises_empty_data_on_non_dict() -> None:
    respx.get(_PRICE_URL).mock(return_value=httpx.Response(200, json=["not", "a", "dict"]))
    with pytest.raises(EmptyDataError):
        polymarket_market_prices(token_id="abc")


def test_polymarket_market_prices_rejects_blank_token() -> None:
    with pytest.raises(InvalidParameterError, match="token_id"):
        polymarket_market_prices(token_id="   ")
