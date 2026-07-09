"""Offline unit tests for the Polymarket connectors (mocked HTTP)."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, ProviderError

from parsimony_polymarket import (
    CONNECTORS,
    polymarket_event,
    polymarket_market,
    polymarket_price_history,
    polymarket_search_events,
)

_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
_EVENT_URL = "https://gamma-api.polymarket.com/events/slug/june-inflation"
_MARKET_URL = "https://gamma-api.polymarket.com/markets/slug/will-cpi"
_HISTORY_URL = "https://clob.polymarket.com/prices-history"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {
        "polymarket_search_events",
        "polymarket_event",
        "polymarket_market",
        "polymarket_price_history",
    }


def test_navigation_verbs_are_enumerators() -> None:
    by_name = {c.name: c for c in CONNECTORS}
    assert "enumerator" in by_name["polymarket_search_events"].tags
    assert "enumerator" in by_name["polymarket_event"].tags
    assert "enumerator" in by_name["polymarket_market"].tags
    # price history is observation data → plain connector, not entity discovery.
    assert "enumerator" not in by_name["polymarket_price_history"].tags


# ---------------------------------------------------------------------------
# polymarket_search_events
# ---------------------------------------------------------------------------


@respx.mock
def test_search_events_returns_declared_columns() -> None:
    respx.get(_SEARCH_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "events": [
                    {
                        "slug": "june-inflation",
                        "title": "June Inflation US",
                        "description": "desc",
                        "markets": [{"x": 1}, {"y": 2}],
                        "volume": "1234.5",
                        "liquidity": "678.9",
                        "active": True,
                        "closed": False,
                        "noise": "dropped",
                    }
                ]
            },
        )
    )
    result = polymarket_search_events(search_text="inflation", limit=5)
    assert result.provenance.source == "polymarket_search_events"
    assert set(result.data.columns) == {
        "slug",
        "title",
        "description",
        "markets_count",
        "volume",
        "liquidity",
        "active",
        "closed",
    }
    row = result.data.iloc[0]
    assert row["slug"] == "june-inflation"
    assert row["markets_count"] == 2
    assert row["volume"] == 1234.5


@respx.mock
def test_search_events_empty_raises_empty_data() -> None:
    respx.get(_SEARCH_URL).mock(return_value=httpx.Response(200, json={"events": []}))
    with pytest.raises(EmptyDataError):
        polymarket_search_events(search_text="inflation")


@respx.mock
def test_search_events_missing_events_key_raises_parse_error() -> None:
    respx.get(_SEARCH_URL).mock(return_value=httpx.Response(200, json={"profiles": []}))
    with pytest.raises(ParseError):
        polymarket_search_events(search_text="inflation")


@respx.mock
def test_search_events_maps_500() -> None:
    respx.get(_SEARCH_URL).mock(return_value=httpx.Response(500, text="err"))
    with pytest.raises(ProviderError):
        polymarket_search_events(search_text="inflation")


def test_search_events_rejects_blank_query() -> None:
    with pytest.raises(InvalidParameterError, match="search_text"):
        polymarket_search_events(search_text="   ")


def test_search_events_rejects_out_of_range_limit() -> None:
    with pytest.raises(InvalidParameterError):
        polymarket_search_events(search_text="x", limit=0)
    with pytest.raises(InvalidParameterError):
        polymarket_search_events(search_text="x", limit=101)


# ---------------------------------------------------------------------------
# polymarket_event
# ---------------------------------------------------------------------------


@respx.mock
def test_event_returns_market_rows() -> None:
    respx.get(_EVENT_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "slug": "june-inflation",
                "markets": [
                    {
                        "slug": "will-cpi",
                        "question": "Will CPI be 3%?",
                        "description": "d",
                        "outcomes": '["Yes", "No"]',
                        "volume": "50.0",
                        "liquidity": "10.0",
                        "active": True,
                        "closed": False,
                    }
                ],
            },
        )
    )
    result = polymarket_event(slug="june-inflation")
    assert result.provenance.source == "polymarket_event"
    assert set(result.data.columns) == {
        "market_slug",
        "market_question",
        "market_description",
        "market_outcomes_count",
        "market_volume",
        "market_liquidity",
        "market_active",
        "market_closed",
    }
    row = result.data.iloc[0]
    assert row["market_slug"] == "will-cpi"
    assert row["market_outcomes_count"] == 2
    assert row["market_volume"] == 50.0


@respx.mock
def test_event_accepts_list_envelope() -> None:
    # Gamma sometimes wraps the slug object in a one-element list.
    respx.get(_EVENT_URL).mock(
        return_value=httpx.Response(
            200,
            json=[{"slug": "june-inflation", "markets": [{"slug": "m", "question": "q"}]}],
        )
    )
    result = polymarket_event(slug="june-inflation")
    assert result.data.iloc[0]["market_slug"] == "m"


@respx.mock
def test_event_no_markets_raises_empty_data() -> None:
    respx.get(_EVENT_URL).mock(return_value=httpx.Response(200, json={"slug": "s", "markets": []}))
    with pytest.raises(EmptyDataError):
        polymarket_event(slug="june-inflation")


@respx.mock
def test_event_error_body_raises_parse_error() -> None:
    respx.get(_EVENT_URL).mock(
        return_value=httpx.Response(200, json={"type": "not found", "error": "x"})
    )
    with pytest.raises(ParseError):
        polymarket_event(slug="june-inflation")


def test_event_rejects_blank_slug() -> None:
    with pytest.raises(InvalidParameterError, match="slug"):
        polymarket_event(slug="  ")


# ---------------------------------------------------------------------------
# polymarket_market
# ---------------------------------------------------------------------------


@respx.mock
def test_market_returns_outcome_tokens() -> None:
    respx.get(_MARKET_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "slug": "will-cpi",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["111", "222"]',
            },
        )
    )
    result = polymarket_market(slug="will-cpi")
    assert result.provenance.source == "polymarket_market"
    assert set(result.data.columns) == {"clob_token_id", "outcome"}
    assert result.data["outcome"].tolist() == ["Yes", "No"]
    assert result.data["clob_token_id"].tolist() == ["111", "222"]


@respx.mock
def test_market_no_tokens_raises_empty_data() -> None:
    respx.get(_MARKET_URL).mock(
        return_value=httpx.Response(200, json={"slug": "s", "outcomes": "[]", "clobTokenIds": "[]"})
    )
    with pytest.raises(EmptyDataError):
        polymarket_market(slug="will-cpi")


@respx.mock
def test_market_missing_outcomes_raises_parse_error() -> None:
    respx.get(_MARKET_URL).mock(
        return_value=httpx.Response(200, json={"type": "err", "error": "no"})
    )
    with pytest.raises(ParseError):
        polymarket_market(slug="will-cpi")


def test_market_rejects_blank_slug() -> None:
    with pytest.raises(InvalidParameterError, match="slug"):
        polymarket_market(slug="")


# ---------------------------------------------------------------------------
# polymarket_price_history
# ---------------------------------------------------------------------------


@respx.mock
def test_price_history_returns_tidy_series() -> None:
    respx.get(_HISTORY_URL).mock(
        return_value=httpx.Response(
            200,
            json={"history": [{"t": 1782979203, "p": "0.0245"}, {"t": 1782982804, "p": "0.0255"}]},
        )
    )
    result = polymarket_price_history(token_id="111", interval="1w")
    assert result.provenance.source == "polymarket_price_history"
    df = result.data
    assert set(df.columns) == {"token", "timestamp", "probability"}
    assert len(df) == 2
    assert df["token"].iloc[0] == "111"
    assert df["probability"].iloc[0] == pytest.approx(0.0245)
    assert str(df["timestamp"].dtype).startswith("datetime64")


@respx.mock
def test_price_history_empty_raises_empty_data() -> None:
    respx.get(_HISTORY_URL).mock(return_value=httpx.Response(200, json={"history": []}))
    with pytest.raises(EmptyDataError):
        polymarket_price_history(token_id="111")


@respx.mock
def test_price_history_missing_history_key_raises_parse_error() -> None:
    respx.get(_HISTORY_URL).mock(return_value=httpx.Response(200, json={"nope": 1}))
    with pytest.raises(ParseError):
        polymarket_price_history(token_id="111")


@respx.mock
def test_price_history_maps_500() -> None:
    respx.get(_HISTORY_URL).mock(return_value=httpx.Response(500, text="err"))
    with pytest.raises(ProviderError):
        polymarket_price_history(token_id="111")


def test_price_history_rejects_blank_token() -> None:
    with pytest.raises(InvalidParameterError, match="token_id"):
        polymarket_price_history(token_id="   ")


def test_price_history_rejects_bad_interval() -> None:
    with pytest.raises(InvalidParameterError, match="interval"):
        polymarket_price_history(token_id="111", interval="2w")


def test_price_history_rejects_bad_fidelity() -> None:
    with pytest.raises(InvalidParameterError, match="fidelity"):
        polymarket_price_history(token_id="111", fidelity=0)
