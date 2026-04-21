"""CoinGecko source: crypto market data via CoinGecko and GeckoTerminal APIs.

API docs: https://docs.coingecko.com/v3.0.1/reference/authentication
Authentication: Demo API key via ``x-cg-demo-api-key`` header.
Base URL: https://api.coingecko.com/api/v3
GeckoTerminal (on-chain): /onchain appended to base URL.
Rate limit: 30 calls/min (Demo plan).

Provides 11 connectors:
  - Discovery: search, trending coins, top gainers/losers
  - Market data: price, markets listing, coin detail
  - Historical: market chart (days or range), OHLC candlesticks
  - On-chain: token price by contract address (GeckoTerminal)
  - Enumerator: full coin list for catalog indexing

Internal layout (not part of the public contract):

* :mod:`parsimony_coingecko._http` — shared transport, unified error
  mapping (401/403, 402, 429, other), plan-restriction body parsing, and
  the ``coingecko_fetch`` helper used by every connector.
* :mod:`parsimony_coingecko.params` — Pydantic parameter models,
  including the path-component validators for coin id, network and
  contract-address interpolation.
* :mod:`parsimony_coingecko.outputs` — declarative
  :class:`OutputConfig` schemas.

This ``__init__.py`` stays at the top level so ``tools/gen_registry.py``
can AST-parse ``@connector`` decorators (it does not follow re-exports).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, ParseError
from parsimony.result import Provenance, Result

from parsimony_coingecko._http import coingecko_fetch as _cg_fetch
from parsimony_coingecko._http import make_http as _make_http
from parsimony_coingecko.outputs import ENUMERATE_OUTPUT as _ENUMERATE_OUTPUT
from parsimony_coingecko.outputs import GAINERS_LOSERS_OUTPUT as _GAINERS_LOSERS_OUTPUT
from parsimony_coingecko.outputs import MARKET_CHART_OUTPUT as _MARKET_CHART_OUTPUT
from parsimony_coingecko.outputs import MARKETS_OUTPUT as _MARKETS_OUTPUT
from parsimony_coingecko.outputs import OHLC_OUTPUT as _OHLC_OUTPUT
from parsimony_coingecko.outputs import ONCHAIN_PRICE_OUTPUT as _ONCHAIN_PRICE_OUTPUT
from parsimony_coingecko.outputs import PRICE_OUTPUT as _PRICE_OUTPUT
from parsimony_coingecko.outputs import SEARCH_OUTPUT as _SEARCH_OUTPUT
from parsimony_coingecko.outputs import TRENDING_OUTPUT as _TRENDING_OUTPUT
from parsimony_coingecko.params import (
    CoinGeckoCoinDetailParams,
    CoinGeckoEnumerateParams,
    CoinGeckoMarketChartParams,
    CoinGeckoMarketChartRangeParams,
    CoinGeckoMarketsParams,
    CoinGeckoOhlcParams,
    CoinGeckoPriceParams,
    CoinGeckoSearchParams,
    CoinGeckoTokenPriceOnchainParams,
    CoinGeckoTopMoversParams,
    CoinGeckoTrendingParams,
)

ENV_VARS: dict[str, str] = {"api_key": "COINGECKO_API_KEY"}

_PROVIDER = "coingecko"
_BASE_URL = "https://api.coingecko.com/api/v3"


def _iso_to_unix(date_str: str) -> int:
    """Convert YYYY-MM-DD ISO date string to Unix timestamp (seconds, UTC)."""
    dt = datetime.fromisoformat(date_str).replace(tzinfo=UTC)
    return int(dt.timestamp())


# ---------------------------------------------------------------------------
# Search / Discovery — Connectors
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["crypto", "tool"])
async def coingecko_search(params: CoinGeckoSearchParams, *, api_key: str) -> Result:
    """Search CoinGecko for coins by name or symbol. Use this first to resolve
    CoinGecko coin IDs before calling coingecko_price, coingecko_markets, or
    coingecko_market_chart. Returns id (the stable identifier), name, symbol,
    and market_cap_rank.

    Example: query='bitcoin' → id='bitcoin'; query='ETH' → id='ethereum'.
    """
    http = _make_http(api_key)
    data = await _cg_fetch(http, path="/search", params={"query": params.query}, op_name="coingecko_search")

    coins = data.get("coins", [])
    if not coins:
        raise EmptyDataError(
            provider="coingecko",
            message=f"No coins found for query: {params.query}",
        )

    rows = [
        {
            "id": c.get("id", ""),
            "name": c.get("name", ""),
            "symbol": c.get("symbol", ""),
            "market_cap_rank": c.get("market_cap_rank"),
            "thumb": c.get("thumb", ""),
        }
        for c in coins
        if c.get("id")
    ]
    df = pd.DataFrame(rows)
    return _SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_search", params={"query": params.query}),
        params={"query": params.query},
    )


@connector(output=_TRENDING_OUTPUT, tags=["crypto", "tool"])
async def coingecko_trending(params: CoinGeckoTrendingParams, *, api_key: str) -> Result:
    """[Demo+] Fetch trending coins on CoinGecko in the last 24 hours.
    Returns top 7 trending coins by search volume, with name, symbol,
    market_cap_rank, and trending score. Use coin id with coingecko_price or
    coingecko_market_chart for live data.
    """
    http = _make_http(api_key)
    data = await _cg_fetch(http, path="/search/trending", op_name="coingecko_trending")

    coins = data.get("coins", [])
    if not coins:
        raise EmptyDataError(provider="coingecko", message="No trending coins returned")

    rows = [
        {
            "id": c["item"].get("id", ""),
            "name": c["item"].get("name", ""),
            "symbol": c["item"].get("symbol", ""),
            "market_cap_rank": c["item"].get("market_cap_rank"),
            "score": c["item"].get("score"),
        }
        for c in coins
        if c.get("item", {}).get("id")
    ]
    df = pd.DataFrame(rows)
    return _TRENDING_OUTPUT.build_table_result(
        df, provenance=Provenance(source="coingecko_trending", params={}), params={}
    )


@connector(output=_GAINERS_LOSERS_OUTPUT, tags=["crypto", "tool"])
async def coingecko_top_gainers_losers(params: CoinGeckoTopMoversParams, *, api_key: str) -> Result:
    """[Demo+] Fetch top gaining and losing coins over a given time window.
    Returns combined rows with a 'direction' column ('gainer' or 'loser') and
    usd_price_percent_change. Use coin id with coingecko_market_chart to dig
    into historical performance.
    """
    http = _make_http(api_key)
    data = await _cg_fetch(
        http,
        path="/coins/top_gainers_losers",
        params={"vs_currency": params.vs_currency, "duration": params.duration, "top_coins": params.top_coins},
        op_name="coingecko_top_gainers_losers",
    )

    rows: list[dict] = []
    for coin in data.get("top_gainers", []):
        rows.append(
            {
                "id": coin.get("id", ""),
                "name": coin.get("name", ""),
                "symbol": coin.get("symbol", ""),
                "direction": "gainer",
                "usd_price_percent_change": coin.get("usd_price_percent_change"),
            }
        )
    for coin in data.get("top_losers", []):
        rows.append(
            {
                "id": coin.get("id", ""),
                "name": coin.get("name", ""),
                "symbol": coin.get("symbol", ""),
                "direction": "loser",
                "usd_price_percent_change": coin.get("usd_price_percent_change"),
            }
        )

    if not rows:
        raise EmptyDataError(provider="coingecko", message="No top gainers/losers returned")

    df = pd.DataFrame(rows)
    return _GAINERS_LOSERS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_top_gainers_losers",
            params={"vs_currency": params.vs_currency, "duration": params.duration},
        ),
        params={"vs_currency": params.vs_currency, "duration": params.duration},
    )


# ---------------------------------------------------------------------------
# Market Data — Connectors
# ---------------------------------------------------------------------------


@connector(output=_PRICE_OUTPUT, tags=["crypto"])
async def coingecko_price(params: CoinGeckoPriceParams, *, api_key: str) -> Result:
    """[Demo+] Fetch current price(s) for one or more coins in one or more currencies.
    Returns one row per coin with dynamic columns: {currency}, {currency}_market_cap,
    {currency}_24h_vol, {currency}_24h_change. Use coingecko_search to resolve coin IDs
    first. For full market rankings use coingecko_markets.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "ids": params.ids,
        "vs_currencies": params.vs_currencies,
        "include_market_cap": str(params.include_market_cap).lower(),
        "include_24hr_vol": str(params.include_24hr_vol).lower(),
        "include_24hr_change": str(params.include_24hr_change).lower(),
    }
    data = await _cg_fetch(http, path="/simple/price", params=req, op_name="coingecko_price")

    if not data:
        raise EmptyDataError(
            provider="coingecko",
            message=f"No price data returned for: {params.ids}",
        )

    rows = [{"id": coin_id, **vals} for coin_id, vals in data.items() if isinstance(vals, dict)]
    if not rows:
        raise EmptyDataError(provider="coingecko", message=f"Empty price response for: {params.ids}")

    df = pd.DataFrame(rows)
    return _PRICE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_price", params={"ids": params.ids, "vs_currencies": params.vs_currencies}
        ),
        params={"ids": params.ids},
    )


@connector(output=_MARKETS_OUTPUT, tags=["crypto"])
async def coingecko_markets(params: CoinGeckoMarketsParams, *, api_key: str) -> Result:
    """[Demo+] Fetch ranked market data for coins: price, market cap, volume, ATH/ATL,
    24h change. Returns up to 250 coins per page sorted by market_cap_desc by default.
    Pass ids= to retrieve specific coins only. Use coingecko_search to resolve coin IDs.
    For time-series history use coingecko_market_chart.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "vs_currency": params.vs_currency,
        "order": params.order,
        "per_page": params.per_page,
        "page": params.page,
        "sparkline": str(params.sparkline).lower(),
    }
    if params.ids:
        req["ids"] = params.ids

    data = await _cg_fetch(http, path="/coins/markets", params=req, op_name="coingecko_markets")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(provider="coingecko", message="No market data returned")

    df = pd.DataFrame(data)
    return _MARKETS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_markets", params={"vs_currency": params.vs_currency, "page": params.page}
        ),
        params={"vs_currency": params.vs_currency},
    )


@connector(tags=["crypto"])
async def coingecko_coin_detail(params: CoinGeckoCoinDetailParams, *, api_key: str) -> Result:
    """[Demo+] Fetch full metadata for a single coin: description, links, categories,
    genesis date, hashing algorithm, current market data, and optional community/
    developer stats. Returns a rich dict — use coingecko_markets for tabular price
    listings across many coins, and coingecko_market_chart for time-series history.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "localization": str(params.localization).lower(),
        "tickers": str(params.tickers).lower(),
        "market_data": str(params.market_data).lower(),
        "community_data": str(params.community_data).lower(),
        "developer_data": str(params.developer_data).lower(),
    }
    data = await _cg_fetch(http, path=f"/coins/{params.coin_id}", params=req, op_name="coingecko_coin_detail")

    if not isinstance(data, dict) or "id" not in data:
        raise ParseError(
            provider="coingecko",
            message=f"Unexpected response structure for coin detail: {params.coin_id}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="coingecko_coin_detail", params={"coin_id": params.coin_id}),
    )


# ---------------------------------------------------------------------------
# Historical Data — Connectors
# ---------------------------------------------------------------------------


@connector(output=_MARKET_CHART_OUTPUT, tags=["crypto"])
async def coingecko_market_chart(params: CoinGeckoMarketChartParams, *, api_key: str) -> Result:
    """[Demo+] Fetch historical price, market cap, and total volume for a coin over
    the last N days. Auto-granularity: 1d→5-min intervals, 2-90d→hourly, 90d+→daily.
    Pass interval='daily' to force daily candles regardless of range. Use
    coingecko_market_chart_range for a precise date range with ISO start/end dates.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"vs_currency": params.vs_currency, "days": params.days}
    if params.interval:
        req["interval"] = params.interval

    data = await _cg_fetch(
        http,
        path=f"/coins/{params.coin_id}/market_chart",
        params=req,
        op_name="coingecko_market_chart",
    )

    df = _build_market_chart_df(data, op_name="coingecko_market_chart")
    return _MARKET_CHART_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_market_chart",
            params={"coin_id": params.coin_id, "vs_currency": params.vs_currency, "days": params.days},
        ),
        params={"coin_id": params.coin_id},
    )


@connector(output=_MARKET_CHART_OUTPUT, tags=["crypto"])
async def coingecko_market_chart_range(params: CoinGeckoMarketChartRangeParams, *, api_key: str) -> Result:
    """[Demo+] Fetch historical price, market cap, and total volume for a coin between
    two ISO dates. More precise than coingecko_market_chart when you need a specific
    date window. Granularity is automatic based on range width (hourly for < 90 days,
    daily for longer). Use from_date='YYYY-MM-DD' and to_date='YYYY-MM-DD'.

    Demo plan: limited to data within the last 365 days (raises PaymentRequiredError
    for older ranges). Use coingecko_market_chart with days='max' on Demo for full
    history (Pro plan removes the restriction).
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {
        "vs_currency": params.vs_currency,
        "from": _iso_to_unix(params.from_date),
        "to": _iso_to_unix(params.to_date),
    }
    data = await _cg_fetch(
        http,
        path=f"/coins/{params.coin_id}/market_chart/range",
        params=req,
        op_name="coingecko_market_chart_range",
    )

    df = _build_market_chart_df(data, op_name="coingecko_market_chart_range")
    return _MARKET_CHART_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_market_chart_range",
            params={
                "coin_id": params.coin_id,
                "vs_currency": params.vs_currency,
                "from": params.from_date,
                "to": params.to_date,
            },
        ),
        params={"coin_id": params.coin_id},
    )


def _build_market_chart_df(data: Any, *, op_name: str) -> pd.DataFrame:
    """Convert CoinGecko market_chart response [[ts, val], ...] arrays into a DataFrame."""
    if not isinstance(data, dict):
        raise ParseError(provider="coingecko", message=f"Unexpected market chart response type from '{op_name}'")

    prices = data.get("prices", [])
    if not prices:
        raise EmptyDataError(provider="coingecko", message=f"No price data returned from '{op_name}'")

    df_price = pd.DataFrame(prices, columns=["timestamp", "price"])
    df_cap = pd.DataFrame(data.get("market_caps", []), columns=["timestamp", "market_cap"])
    df_vol = pd.DataFrame(data.get("total_volumes", []), columns=["timestamp", "total_volume"])

    df = df_price.merge(df_cap, on="timestamp", how="left").merge(df_vol, on="timestamp", how="left")
    # Convert ms → s so OutputConfig dtype="timestamp" can parse correctly (expects seconds)
    df["timestamp"] = df["timestamp"] / 1000
    return df


@connector(output=_OHLC_OUTPUT, tags=["crypto"])
async def coingecko_ohlc(params: CoinGeckoOhlcParams, *, api_key: str) -> Result:
    """[Demo+] Fetch OHLC (open-high-low-close) candlestick data for a coin.
    Candlestick body: 1-2d→30-min candles, 3-30d→4-hour candles, 31-365d→4-day candles.
    Use coingecko_market_chart for continuous price history with market cap and volume.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"vs_currency": params.vs_currency, "days": params.days}
    data = await _cg_fetch(
        http,
        path=f"/coins/{params.coin_id}/ohlc",
        params=req,
        op_name="coingecko_ohlc",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(provider="coingecko", message=f"No OHLC data returned for {params.coin_id}")

    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
    # Convert ms → s for OutputConfig dtype="timestamp"
    df["timestamp"] = df["timestamp"] / 1000
    return _OHLC_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="coingecko_ohlc",
            params={"coin_id": params.coin_id, "vs_currency": params.vs_currency, "days": params.days},
        ),
        params={"coin_id": params.coin_id},
    )


# ---------------------------------------------------------------------------
# On-Chain / GeckoTerminal — Connectors
# ---------------------------------------------------------------------------


@connector(output=_ONCHAIN_PRICE_OUTPUT, tags=["crypto", "onchain"])
async def coingecko_token_price_onchain(params: CoinGeckoTokenPriceOnchainParams, *, api_key: str) -> Result:
    """[Demo+] Fetch on-chain token price by contract address via GeckoTerminal.
    Use for long-tail tokens not listed on CoinGecko's main index. Prefer
    coingecko_price for well-known assets. Supports multiple addresses in a
    single call (comma-separated). Returns one row per address with price_usd.
    """
    http = _make_http(api_key)
    path = f"/onchain/simple/networks/{params.network}/token_price/{params.contract_addresses}"
    data = await _cg_fetch(
        http, path=path, params={"vs_currencies": params.vs_currencies}, op_name="coingecko_token_price_onchain"
    )

    token_prices: dict = {}
    try:
        token_prices = data.get("data", {}).get("attributes", {}).get("token_prices", {})
    except AttributeError as exc:
        raise ParseError(provider="coingecko", message="Unexpected on-chain price response structure") from exc

    if not token_prices:
        raise EmptyDataError(
            provider="coingecko",
            message=f"No on-chain price data for addresses: {params.contract_addresses}",
        )

    rows = [{"contract_address": addr, "price_usd": float(price)} for addr, price in token_prices.items()]
    df = pd.DataFrame(rows)
    return _ONCHAIN_PRICE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="coingecko_token_price_onchain", params={"network": params.network}),
        params={"network": params.network},
    )


# ---------------------------------------------------------------------------
# Enumerator — full coin list for catalog indexing
# ---------------------------------------------------------------------------


@enumerator(output=_ENUMERATE_OUTPUT, tags=["crypto"])
async def enumerate_coingecko(params: CoinGeckoEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate all coins from CoinGecko for catalog indexing.

    Calls /coins/list — returns ~15 000 rows with id, name, symbol.
    Used to build the parsimony catalog for offline search without
    hitting the rate limit.
    """
    async with httpx.AsyncClient(
        headers={"x-cg-demo-api-key": api_key},
        timeout=60.0,
    ) as client:
        resp = await client.get(
            f"{_BASE_URL}/coins/list",
            params={"include_platform": str(params.include_platform).lower()},
        )
        resp.raise_for_status()
        data: list[dict] = resp.json()

    if not data:
        return pd.DataFrame(columns=["id", "name", "symbol", "platforms"])

    rows = [
        {
            "id": c.get("id", ""),
            "name": c.get("name", c.get("id", "")),
            "symbol": c.get("symbol", ""),
            "platforms": str(c.get("platforms", {})) if c.get("platforms") else "",
        }
        for c in data
        if c.get("id")
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Collection — kept as a literal ``Connectors([...])`` assignment so
# ``tools/gen_registry.py`` can AST-extract it.
# ---------------------------------------------------------------------------

CONNECTORS = Connectors(
    [
        # Discovery
        coingecko_search,
        coingecko_trending,
        coingecko_top_gainers_losers,
        # Fetch
        coingecko_price,
        coingecko_markets,
        coingecko_coin_detail,
        coingecko_market_chart,
        coingecko_market_chart_range,
        coingecko_ohlc,
        coingecko_token_price_onchain,
        # Enumeration
        enumerate_coingecko,
    ]
)


__all__ = [
    "CONNECTORS",
    "ENV_VARS",
    # Parameter models (public — downstream callers type against these)
    "CoinGeckoCoinDetailParams",
    "CoinGeckoEnumerateParams",
    "CoinGeckoMarketChartParams",
    "CoinGeckoMarketChartRangeParams",
    "CoinGeckoMarketsParams",
    "CoinGeckoOhlcParams",
    "CoinGeckoPriceParams",
    "CoinGeckoSearchParams",
    "CoinGeckoTokenPriceOnchainParams",
    "CoinGeckoTopMoversParams",
    "CoinGeckoTrendingParams",
    # Connector functions
    "coingecko_coin_detail",
    "coingecko_market_chart",
    "coingecko_market_chart_range",
    "coingecko_markets",
    "coingecko_ohlc",
    "coingecko_price",
    "coingecko_search",
    "coingecko_token_price_onchain",
    "coingecko_top_gainers_losers",
    "coingecko_trending",
    # Enumerator
    "enumerate_coingecko",
]
