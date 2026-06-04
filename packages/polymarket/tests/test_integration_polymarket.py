"""Live integration tests for parsimony-polymarket.

Hits the real public Gamma (``https://gamma-api.polymarket.com``) and CLOB
(``https://clob.polymarket.com``) read APIs. Skipped by default — the root
``pyproject.toml`` sets ``-m 'not integration'``. Run explicitly with::

    uv run pytest packages/polymarket -m integration

No credentials required — both APIs are public, so these tests need no env
vars and run in CI without secrets.
"""

from __future__ import annotations

import json

import httpx
import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_polymarket import polymarket_events, polymarket_market_prices, polymarket_markets

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_polymarket_markets_live() -> None:
    result = await polymarket_markets(limit=5)

    assert_provenance_shape(result, expected_source="polymarket_markets", required_param_keys=["limit"])
    df = result.data
    assert not df.empty, "Gamma /markets returned an empty DataFrame"
    assert list(df.columns) == ["id", "question", "slug", "active"]
    # Real content, not just column names: ids and questions must be populated.
    assert df["id"].astype(str).str.len().gt(0).all(), "blank market id"
    assert df["question"].astype(str).str.len().gt(0).any(), "no real market question text"
    assert df["slug"].astype(str).str.len().gt(0).any(), "no real market slug"
    assert len(df) <= 5, "limit not respected"


@pytest.mark.asyncio
async def test_polymarket_events_live() -> None:
    result = await polymarket_events(limit=5)

    assert_provenance_shape(result, expected_source="polymarket_events", required_param_keys=["limit"])
    df = result.data
    assert not df.empty, "Gamma /events returned an empty DataFrame"
    assert list(df.columns) == ["id", "title", "slug"]
    assert df["id"].astype(str).str.len().gt(0).all(), "blank event id"
    assert df["title"].astype(str).str.len().gt(0).any(), "no real event title text"
    assert df["slug"].astype(str).str.len().gt(0).any(), "no real event slug"
    assert len(df) <= 5, "limit not respected"


@pytest.mark.asyncio
async def test_polymarket_market_prices_live() -> None:
    # Derive a real CLOB token id from a live market (the prices endpoint needs a
    # concrete clobTokenIds value — there is no static fixture id we can rely on).
    token_id = await _live_clob_token_id()

    result = await polymarket_market_prices(token_id=token_id)

    assert_provenance_shape(result, expected_source="polymarket_market_prices", required_param_keys=["token_id"])
    data = result.data
    assert isinstance(data, dict), f"expected a price dict, got {type(data)!r}"
    assert "price" in data, f"price key missing from CLOB response: {data!r}"
    # The price is a stringified float in [0, 1] for a binary outcome token.
    price = float(data["price"])
    assert 0.0 <= price <= 1.0, f"CLOB price out of [0,1] range: {price}"


async def _live_clob_token_id() -> str:
    """Pull one real CLOB token id from a live, order-book-enabled market."""
    async with httpx.AsyncClient(base_url="https://gamma-api.polymarket.com", timeout=15.0) as client:
        resp = await client.get("/markets", params={"limit": 20, "active": "true"})
        resp.raise_for_status()
        markets = resp.json()

    for market in markets:
        raw = market.get("clobTokenIds")
        if not raw:
            continue
        try:
            token_ids = json.loads(raw) if isinstance(raw, str) else raw
        except (ValueError, TypeError):
            continue
        if token_ids:
            return str(token_ids[0])

    pytest.skip("no live market exposed a clobTokenIds value to price")
