"""Happy-path tests for the CoinGecko connectors.

Follows ``docs/testing-template.md``. CoinGecko connectors take an ``api_key``
keyword dep → we exercise the template's 401/429 error-mapping contract and
assert the api-key value never appears in raised-exception strings.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import RateLimitError, UnauthorizedError

from parsimony_coingecko import (
    CONNECTORS,
    ENV_VARS,
    CoinGeckoEnumerateParams,
    CoinGeckoMarketChartParams,
    CoinGeckoMarketsParams,
    CoinGeckoPriceParams,
    CoinGeckoSearchParams,
    coingecko_market_chart,
    coingecko_markets,
    coingecko_price,
    coingecko_search,
    enumerate_coingecko,
)

_KEY = "live-looking-key-abc123"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "COINGECKO_API_KEY"}


def test_connectors_count_matches_docstring() -> None:
    assert len(CONNECTORS) == 11


def test_tool_tagged_connectors_have_long_first_line() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


# ---------------------------------------------------------------------------
# coingecko_search — tool-tagged, covers the error-mapping contract
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_coingecko_search_returns_coin_rows() -> None:
    respx.get("https://api.coingecko.com/api/v3/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "coins": [
                    {"id": "bitcoin", "name": "Bitcoin", "symbol": "BTC", "market_cap_rank": 1, "thumb": "btc.png"},
                    {"id": "ethereum", "name": "Ethereum", "symbol": "ETH", "market_cap_rank": 2, "thumb": "eth.png"},
                ]
            },
        )
    )

    bound = coingecko_search.bind_deps(api_key=_KEY)
    result = await bound(CoinGeckoSearchParams(query="btc"))

    assert result.provenance.source == "coingecko_search"
    assert list(result.data["id"]) == ["bitcoin", "ethereum"]


@respx.mock
@pytest.mark.asyncio
async def test_coingecko_search_maps_401_to_unauthorized_without_leaking_key() -> None:
    respx.get("https://api.coingecko.com/api/v3/search").mock(
        return_value=httpx.Response(401, json={"status": {"error_code": 1001, "error_message": "invalid key"}})
    )

    bound = coingecko_search.bind_deps(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(CoinGeckoSearchParams(query="x"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_coingecko_search_maps_429_to_rate_limit_without_leaking_key() -> None:
    respx.get("https://api.coingecko.com/api/v3/search").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "10"}, json={"error": "rate limited"})
    )

    bound = coingecko_search.bind_deps(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(CoinGeckoSearchParams(query="x"))
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# coingecko_price
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_coingecko_price_returns_rows_per_coin() -> None:
    respx.get("https://api.coingecko.com/api/v3/simple/price").mock(
        return_value=httpx.Response(
            200,
            json={
                "bitcoin": {"usd": 65000.0, "usd_market_cap": 1.28e12, "usd_24h_vol": 3.0e10, "usd_24h_change": 1.2},
            },
        )
    )

    bound = coingecko_price.bind_deps(api_key=_KEY)
    result = await bound(CoinGeckoPriceParams(ids="bitcoin"))

    df = result.data
    assert list(df["id"]) == ["bitcoin"]
    assert df.iloc[0]["usd"] == 65000.0


# ---------------------------------------------------------------------------
# coingecko_markets
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_coingecko_markets_returns_ranked_rows() -> None:
    respx.get("https://api.coingecko.com/api/v3/coins/markets").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": "bitcoin",
                    "name": "Bitcoin",
                    "symbol": "btc",
                    "market_cap_rank": 1,
                    "current_price": 65000.0,
                    "market_cap": 1.28e12,
                    "total_volume": 3.0e10,
                    "high_24h": 66000.0,
                    "low_24h": 64000.0,
                    "price_change_percentage_24h": 1.2,
                    "ath": 73000.0,
                    "atl": 0.049,
                    "circulating_supply": 19_700_000,
                    "total_supply": 21_000_000,
                    "last_updated": "2026-04-20T10:00:00Z",
                }
            ],
        )
    )

    bound = coingecko_markets.bind_deps(api_key=_KEY)
    result = await bound(CoinGeckoMarketsParams())

    df = result.data
    assert df.iloc[0]["id"] == "bitcoin"


# ---------------------------------------------------------------------------
# coingecko_market_chart
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_coingecko_market_chart_merges_price_cap_volume() -> None:
    respx.get("https://api.coingecko.com/api/v3/coins/bitcoin/market_chart").mock(
        return_value=httpx.Response(
            200,
            json={
                "prices": [[1_700_000_000_000, 40000.0], [1_700_000_600_000, 40100.0]],
                "market_caps": [[1_700_000_000_000, 7.6e11], [1_700_000_600_000, 7.6e11]],
                "total_volumes": [[1_700_000_000_000, 1.5e10], [1_700_000_600_000, 1.4e10]],
            },
        )
    )

    bound = coingecko_market_chart.bind_deps(api_key=_KEY)
    result = await bound(CoinGeckoMarketChartParams(coin_id="bitcoin", days="1"))

    df = result.data
    assert len(df) == 2
    assert "market_cap" in df.columns


# ---------------------------------------------------------------------------
# enumerate_coingecko — uses raw httpx.AsyncClient internally
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_coingecko_emits_catalog_rows() -> None:
    respx.get("https://api.coingecko.com/api/v3/coins/list").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"id": "bitcoin", "name": "Bitcoin", "symbol": "btc"},
                {"id": "ethereum", "name": "Ethereum", "symbol": "eth"},
            ],
        )
    )

    bound = enumerate_coingecko.bind_deps(api_key=_KEY)
    result = await bound(CoinGeckoEnumerateParams())

    df = result.data
    assert len(df) == 2
    assert set(df["id"]) == {"bitcoin", "ethereum"}


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_search_params_requires_non_empty_query() -> None:
    with pytest.raises(ValueError):
        CoinGeckoSearchParams(query="")


def test_coin_id_namespace_rejects_path_traversal() -> None:
    with pytest.raises(ValueError):
        CoinGeckoMarketChartParams(coin_id="../etc/passwd", days="1")
