"""Happy-path tests for the Polymarket connectors."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import InvalidParameterError, ProviderError

from parsimony_polymarket import CONNECTORS, polymarket_events, polymarket_market_prices, polymarket_markets


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"polymarket_markets", "polymarket_events", "polymarket_market_prices"}


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_markets_returns_rows() -> None:
    respx.get("https://gamma-api.polymarket.com/markets").mock(
        return_value=httpx.Response(
            200,
            json=[{"id": "1", "question": "Will X happen?", "slug": "x", "active": True}],
        )
    )
    result = await polymarket_markets(limit=5)
    assert result.provenance.source == "polymarket_markets"
    assert result.data.iloc[0]["slug"] == "x"


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_events_returns_rows() -> None:
    respx.get("https://gamma-api.polymarket.com/events").mock(
        return_value=httpx.Response(200, json=[{"id": "e1", "title": "Election", "slug": "election"}])
    )
    result = await polymarket_events(limit=5)
    assert result.provenance.source == "polymarket_events"


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_market_prices() -> None:
    respx.get("https://clob.polymarket.com/price").mock(
        return_value=httpx.Response(200, json={"price": "0.42"})
    )
    result = await polymarket_market_prices(token_id="abc")
    assert result.data["price"] == "0.42"


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_markets_maps_500() -> None:
    respx.get("https://gamma-api.polymarket.com/markets").mock(return_value=httpx.Response(500, text="err"))
    with pytest.raises(ProviderError):
        await polymarket_markets(limit=5)


def test_limit_validation() -> None:
    with pytest.raises(InvalidParameterError):
        import asyncio

        asyncio.run(polymarket_markets(limit=0))
