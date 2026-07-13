"""CoinGecko source: crypto market data via CoinGecko and GeckoTerminal APIs.

API docs: https://docs.coingecko.com/v3.0.1/reference/authentication
Authentication: Demo API key via the ``x-cg-demo-api-key`` request header.
Base URL: https://api.coingecko.com/api/v3
GeckoTerminal (on-chain): reached via the ``/onchain/...`` path prefix on the
same base.
Rate limit: 30 calls/min (Demo plan).

Provides 11 connectors:

* Discovery: ``coingecko_search``, ``coingecko_trending``,
  ``coingecko_top_gainers_losers``.
* Market data: ``coingecko_price``, ``coingecko_markets``,
  ``coingecko_coin_detail``.
* Historical: ``coingecko_market_chart``, ``coingecko_market_chart_range``,
  ``coingecko_ohlc``.
* On-chain: ``coingecko_token_price_onchain`` (GeckoTerminal).
* Enumerator: ``enumerate_coingecko`` (full coin list for catalog indexing).

The API key is declared as a secret (stripped from provenance) and supplied
either by binding (``load(api_key=...)`` / ``Connector.bind``) or, as a dev
fallback, from the ``COINGECKO_API_KEY`` environment variable. A missing key
fails fast with :class:`UnauthorizedError` naming the env var.

Several endpoints are PRO-only or range-restricted on the Demo plan and return
**401** with a plan-restriction ``error_code`` in the body
(``coingecko_top_gainers_losers``; ``coingecko_market_chart_range`` beyond 365
days) — these surface as :class:`PaymentRequiredError`, distinguished from a
genuinely broken key (also 401) by the body's ``error_code`` (see
:mod:`parsimony_coingecko._http`).

Internal layout (not part of the public contract):

* :mod:`parsimony_coingecko._http` — keyed client builder and the unified
  error mapper (401-body-disambiguation, plan-restriction codes).
* :mod:`parsimony_coingecko.outputs` — declarative :class:`OutputSpec`
  schemas.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_coingecko._http import _client
from parsimony_coingecko._http import coingecko_fetch as _cg_fetch
from parsimony_coingecko.outputs import (
    ENUMERATE_OUTPUT,
    GAINERS_LOSERS_OUTPUT,
    MARKET_CHART_OUTPUT,
    MARKETS_OUTPUT,
    OHLC_OUTPUT,
    ONCHAIN_PRICE_OUTPUT,
    PRICE_OUTPUT,
    SEARCH_OUTPUT,
    TRENDING_OUTPUT,
)

__all__ = ["CONNECTORS", "load"]

_PROVIDER = "coingecko"

# The enumerator's /coins/list returns ~17k rows in one call; give it headroom.
_ENUMERATE_TIMEOUT = 60.0

# Regex guards for values interpolated directly into request paths. Anything
# outside the allowed character set is rejected before the URL is built.
_PATH_SAFE_RE = re.compile(r"^[a-zA-Z0-9._\-]+$")
_NETWORK_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")
_CONTRACT_ADDR_RE = re.compile(r"^[a-zA-Z0-9x,]+$")

_ENUMERATE_COLS = ["id", "name", "symbol", "platforms"]


def _iso_to_unix(date_str: str) -> int:
    """Convert a YYYY-MM-DD ISO date string to a Unix timestamp (seconds, UTC)."""
    try:
        dt = datetime.fromisoformat(date_str).replace(tzinfo=UTC)
    except ValueError as exc:
        raise InvalidParameterError(_PROVIDER, f"invalid ISO date: {date_str!r}") from exc
    return int(dt.timestamp())


def _safe_coin_id(coin_id: str) -> str:
    """Validate a path-interpolated coin id; raise InvalidParameterError if unsafe."""
    c = coin_id.strip()
    if not c:
        raise InvalidParameterError(_PROVIDER, "coin_id must be non-empty")
    if not _PATH_SAFE_RE.match(c):
        raise InvalidParameterError(_PROVIDER, f"coin_id contains unsafe characters for URL path: {coin_id!r}")
    return c


# ---------------------------------------------------------------------------
# Search / Discovery
# ---------------------------------------------------------------------------


@connector(output=SEARCH_OUTPUT, tags=["crypto", "tool"], secrets=("api_key",))
def coingecko_search(query: str, api_key: str = "") -> pd.DataFrame:
    """Search CoinGecko for coins by name or symbol. Use this first to resolve
    CoinGecko coin IDs before calling coingecko_price, coingecko_markets, or
    coingecko_market_chart. Returns id (the stable identifier), name, symbol,
    and market_cap_rank.

    Example: query='bitcoin' → id='bitcoin'; query='ETH' → id='ethereum'.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError(_PROVIDER, "query must be non-empty")

    http = _client(api_key)
    data = _cg_fetch(http, path="search", params={"query": q}, op_name="coingecko_search")

    coins = data.get("coins", []) if isinstance(data, dict) else []
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
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"query": q})
    return pd.DataFrame(rows)


@connector(output=TRENDING_OUTPUT, tags=["crypto", "tool"], secrets=("api_key",))
def coingecko_trending(api_key: str = "") -> pd.DataFrame:
    """[Demo+] Fetch trending coins on CoinGecko in the last 24 hours.
    Returns the top 7 trending coins by search volume, with name, symbol,
    market_cap_rank, and a trending score. Use the coin id with coingecko_price
    or coingecko_market_chart for live data.
    """
    http = _client(api_key)
    data = _cg_fetch(http, path="search/trending", op_name="coingecko_trending")

    coins = data.get("coins", []) if isinstance(data, dict) else []
    rows = [
        {
            "id": c["item"].get("id", ""),
            "name": c["item"].get("name", ""),
            "symbol": c["item"].get("symbol", ""),
            "market_cap_rank": c["item"].get("market_cap_rank"),
            "score": c["item"].get("score"),
        }
        for c in coins
        if isinstance(c.get("item"), dict) and c["item"].get("id")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={})
    return pd.DataFrame(rows)


@connector(output=GAINERS_LOSERS_OUTPUT, tags=["crypto", "tool"], secrets=("api_key",))
def coingecko_top_gainers_losers(
    vs_currency: str = "usd",
    duration: Literal["1h", "24h", "7d", "14d", "30d", "60d", "1y"] = "24h",
    top_coins: Literal["300", "1000"] = "1000",
    api_key: str = "",
) -> pd.DataFrame:
    """[PRO] Fetch the top gaining and losing coins over a given time window.
    Returns combined rows with a 'direction' column ('gainer' or 'loser') and
    usd_price_percent_change. This endpoint is PRO-only — a Demo key returns
    PaymentRequiredError. Use the coin id with coingecko_market_chart to dig
    into historical performance.
    """
    http = _client(api_key)
    data = _cg_fetch(
        http,
        path="coins/top_gainers_losers",
        params={"vs_currency": vs_currency, "duration": duration, "top_coins": top_coins},
        op_name="coingecko_top_gainers_losers",
    )
    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "top gainers/losers response was not a JSON object")

    rows: list[dict[str, Any]] = []
    for direction, field in (("gainer", "top_gainers"), ("loser", "top_losers")):
        for coin in data.get(field, []):
            rows.append(
                {
                    "id": coin.get("id", ""),
                    "name": coin.get("name", ""),
                    "symbol": coin.get("symbol", ""),
                    "direction": direction,
                    "usd_price_percent_change": coin.get("usd_price_percent_change"),
                }
            )

    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"vs_currency": vs_currency, "duration": duration})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


@connector(output=PRICE_OUTPUT, tags=["crypto"], secrets=("api_key",))
def coingecko_price(
    ids: str,
    vs_currencies: str = "usd",
    include_market_cap: bool = True,
    include_24hr_vol: bool = True,
    include_24hr_change: bool = True,
    api_key: str = "",
) -> pd.DataFrame:
    """[Demo+] Fetch current price(s) for one or more coins in one or more currencies.
    Returns one row per coin with dynamic columns: {currency}, {currency}_market_cap,
    {currency}_24h_vol, {currency}_24h_change. Use coingecko_search to resolve coin IDs
    first. For full market rankings use coingecko_markets.
    """
    coins = ids.strip()
    if not coins:
        raise InvalidParameterError(_PROVIDER, "ids must be non-empty")

    http = _client(api_key)
    req = {
        "ids": coins,
        "vs_currencies": vs_currencies,
        "include_market_cap": str(include_market_cap).lower(),
        "include_24hr_vol": str(include_24hr_vol).lower(),
        "include_24hr_change": str(include_24hr_change).lower(),
    }
    data = _cg_fetch(http, path="simple/price", params=req, op_name="coingecko_price")

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "price response was not a JSON object")
    rows = [{"id": coin_id, **vals} for coin_id, vals in data.items() if isinstance(vals, dict)]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"ids": coins})
    return pd.DataFrame(rows)


@connector(output=MARKETS_OUTPUT, tags=["crypto"], secrets=("api_key",))
def coingecko_markets(
    vs_currency: str = "usd",
    ids: str | None = None,
    order: Literal[
        "market_cap_desc", "market_cap_asc", "volume_desc", "volume_asc", "id_desc", "id_asc"
    ] = "market_cap_desc",
    per_page: int = 100,
    page: int = 1,
    sparkline: bool = False,
    api_key: str = "",
) -> pd.DataFrame:
    """[Demo+] Fetch ranked market data for coins: price, market cap, volume, ATH/ATL,
    24h change. Returns up to 250 coins per page sorted by market_cap_desc by default.
    Pass ids= to retrieve specific coins only. Use coingecko_search to resolve coin IDs.
    For time-series history use coingecko_market_chart.
    """
    if per_page < 1 or per_page > 250:
        raise InvalidParameterError(_PROVIDER, "per_page must be between 1 and 250")
    if page < 1:
        raise InvalidParameterError(_PROVIDER, "page must be >= 1")

    http = _client(api_key)
    req: dict[str, Any] = {
        "vs_currency": vs_currency,
        "order": order,
        "per_page": per_page,
        "page": page,
        "sparkline": str(sparkline).lower(),
        "ids": ids.strip() if ids and ids.strip() else None,
    }
    data = _cg_fetch(http, path="coins/markets", params=req, op_name="coingecko_markets")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(_PROVIDER, query_params={"vs_currency": vs_currency})
    return pd.DataFrame(data)


@connector(tags=["crypto"], secrets=("api_key",))
def coingecko_coin_detail(
    coin_id: Annotated[str, Namespace("coingecko_coin")],
    localization: bool = False,
    tickers: bool = False,
    market_data: bool = True,
    community_data: bool = False,
    developer_data: bool = False,
    api_key: str = "",
) -> dict[str, Any]:
    """[Demo+] Fetch full metadata for a single coin: description, links, categories,
    genesis date, hashing algorithm, current market data, and optional community/
    developer stats. Returns a rich dict — use coingecko_markets for tabular price
    listings across many coins, and coingecko_market_chart for time-series history.

    Nested shape: market_data figures (current_price, market_cap, total_volume,
    ath, …) are dicts keyed by ~60 currency codes — index one, e.g.
    result.data['market_data']['current_price']['usd'].
    """
    c = _safe_coin_id(coin_id)

    http = _client(api_key)
    req = {
        "localization": str(localization).lower(),
        "tickers": str(tickers).lower(),
        "market_data": str(market_data).lower(),
        "community_data": str(community_data).lower(),
        "developer_data": str(developer_data).lower(),
    }
    data = _cg_fetch(http, path=f"coins/{c}", params=req, op_name="coingecko_coin_detail")

    if not isinstance(data, dict) or "id" not in data:
        raise ParseError(_PROVIDER, f"unexpected coin detail response structure for {c!r}")
    return data


# ---------------------------------------------------------------------------
# Historical data
# ---------------------------------------------------------------------------


def _build_market_chart_df(data: Any, *, op_name: str) -> pd.DataFrame:
    """Convert a CoinGecko market_chart response ([[ts, val], ...]) into a DataFrame."""
    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, f"unexpected market chart response type from {op_name!r}")

    prices = data.get("prices", [])
    if not prices:
        raise EmptyDataError(_PROVIDER, query_params={"op_name": op_name})

    df_price = pd.DataFrame(prices, columns=["timestamp", "price"])
    df_cap = pd.DataFrame(data.get("market_caps", []), columns=["timestamp", "market_cap"])
    df_vol = pd.DataFrame(data.get("total_volumes", []), columns=["timestamp", "total_volume"])

    df = df_price.merge(df_cap, on="timestamp", how="left").merge(df_vol, on="timestamp", how="left")
    # CoinGecko timestamps are epoch milliseconds.
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


@connector(output=MARKET_CHART_OUTPUT, tags=["crypto"], secrets=("api_key",))
def coingecko_market_chart(
    coin_id: Annotated[str, Namespace("coingecko_coin")],
    days: str,
    vs_currency: str = "usd",
    interval: Literal["5m", "hourly", "daily"] | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Demo+] Fetch historical price, market cap, and total volume for a coin over
    the last N days. Auto-granularity: 1d→5-min intervals, 2-90d→hourly, 90d+→daily.
    Pass interval='daily' to force daily candles regardless of range. Use
    coingecko_market_chart_range for a precise date range with ISO start/end dates.

    Demo plan: days is capped at 365 — days>365 (and days='max') return
    PaymentRequiredError, so the deepest Demo history is days='365'. The Pro plan
    removes the cap.
    """
    c = _safe_coin_id(coin_id)
    if not days.strip():
        raise InvalidParameterError(_PROVIDER, "days must be non-empty (an integer or 'max')")

    http = _client(api_key)
    req: dict[str, Any] = {"vs_currency": vs_currency, "days": days.strip(), "interval": interval}
    data = _cg_fetch(http, path=f"coins/{c}/market_chart", params=req, op_name="coingecko_market_chart")
    return _build_market_chart_df(data, op_name="coingecko_market_chart")


@connector(output=MARKET_CHART_OUTPUT, tags=["crypto"], secrets=("api_key",))
def coingecko_market_chart_range(
    coin_id: Annotated[str, Namespace("coingecko_coin")],
    from_date: str,
    to_date: str,
    vs_currency: str = "usd",
    api_key: str = "",
) -> pd.DataFrame:
    """[Demo+] Fetch historical price, market cap, and total volume for a coin between
    two ISO dates. More precise than coingecko_market_chart when you need a specific
    date window. Granularity is automatic based on range width (hourly for < 90 days,
    daily for longer). Use from_date='YYYY-MM-DD' and to_date='YYYY-MM-DD'.

    Demo plan: limited to data within the last 365 days (older ranges return
    PaymentRequiredError). coingecko_market_chart is capped the same way on Demo
    (days>365 / days='max' → PaymentRequiredError), so neither entry point reaches
    full history there; the Pro plan removes the cap on both.
    """
    c = _safe_coin_id(coin_id)

    http = _client(api_key)
    req: dict[str, Any] = {
        "vs_currency": vs_currency,
        "from": _iso_to_unix(from_date),
        "to": _iso_to_unix(to_date),
    }
    data = _cg_fetch(http, path=f"coins/{c}/market_chart/range", params=req, op_name="coingecko_market_chart_range")
    return _build_market_chart_df(data, op_name="coingecko_market_chart_range")


@connector(output=OHLC_OUTPUT, tags=["crypto"], secrets=("api_key",))
def coingecko_ohlc(
    coin_id: Annotated[str, Namespace("coingecko_coin")],
    vs_currency: str = "usd",
    days: Literal[1, 7, 14, 30, 90, 180, 365] = 30,
    api_key: str = "",
) -> pd.DataFrame:
    """[Demo+] Fetch OHLC (open-high-low-close) candlestick data for a coin.
    Candlestick body: 1-2d→30-min candles, 3-30d→4-hour candles, 31-365d→4-day candles.
    Use coingecko_market_chart for continuous price history with market cap and volume.
    """
    c = _safe_coin_id(coin_id)

    http = _client(api_key)
    req: dict[str, Any] = {"vs_currency": vs_currency, "days": days}
    data = _cg_fetch(http, path=f"coins/{c}/ohlc", params=req, op_name="coingecko_ohlc")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(_PROVIDER, query_params={"coin_id": c, "days": days})
    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
    # CoinGecko timestamps are epoch milliseconds.
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


# ---------------------------------------------------------------------------
# On-chain / GeckoTerminal
# ---------------------------------------------------------------------------


@connector(output=ONCHAIN_PRICE_OUTPUT, tags=["crypto", "onchain"], secrets=("api_key",))
def coingecko_token_price_onchain(
    network: str,
    contract_addresses: str,
    vs_currencies: str = "usd",
    api_key: str = "",
) -> pd.DataFrame:
    """[Demo+] Fetch on-chain token price by contract address via GeckoTerminal.
    Use for long-tail tokens not listed on CoinGecko's main index. Prefer
    coingecko_price for well-known assets. Supports multiple addresses in a
    single call (comma-separated). Returns one row per address with price_usd.
    """
    net = network.strip()
    if not _NETWORK_RE.match(net):
        raise InvalidParameterError(_PROVIDER, f"network contains unsafe characters for URL path: {network!r}")
    addrs = contract_addresses.strip()
    if not _CONTRACT_ADDR_RE.match(addrs):
        raise InvalidParameterError(
            _PROVIDER, f"contract_addresses contains unsafe characters for URL path: {contract_addresses!r}"
        )

    http = _client(api_key)
    path = f"onchain/simple/networks/{net}/token_price/{addrs}"
    data = _cg_fetch(http, path=path, params={"vs_currencies": vs_currencies}, op_name="coingecko_token_price_onchain")

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "unexpected on-chain price response structure")
    token_prices = data.get("data", {}).get("attributes", {}).get("token_prices", {})
    if not isinstance(token_prices, dict) or not token_prices:
        raise EmptyDataError(_PROVIDER, query_params={"network": net, "contract_addresses": addrs})

    rows = [{"contract_address": addr, "price_usd": price} for addr, price in token_prices.items()]
    df = pd.DataFrame(rows)
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Enumerator — full coin list for catalog indexing
# ---------------------------------------------------------------------------


@enumerator(output=ENUMERATE_OUTPUT, tags=["crypto"], secrets=("api_key",))
def enumerate_coingecko(include_platform: bool = True, api_key: str = "") -> pd.DataFrame:
    """Enumerate all coins from CoinGecko for catalog indexing.

    Calls /coins/list — returns ~17 000 rows with id, name, symbol, and (when
    include_platform=True, the default) contract-address platforms. Used to
    build the parsimony catalog for offline search without hitting the rate
    limit. Routed through the package transport so a 401/429/5xx during the
    build surfaces as a typed connector error.
    """
    http = _client(api_key, timeout=_ENUMERATE_TIMEOUT)
    data = _cg_fetch(
        http,
        path="coins/list",
        params={"include_platform": str(include_platform).lower()},
        op_name="enumerate_coingecko",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "coin list response was not a JSON array")
    rows = [
        {
            "id": c.get("id", ""),
            "name": c.get("name", c.get("id", "")),
            "symbol": c.get("symbol", ""),
            "platforms": str(c["platforms"]) if c.get("platforms") else "",
        }
        for c in data
        if c.get("id")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={})
    return pd.DataFrame(rows, columns=_ENUMERATE_COLS)


CONNECTORS = Connectors(
    [
        # Discovery
        coingecko_search,
        coingecko_trending,
        coingecko_top_gainers_losers,
        # Market data
        coingecko_price,
        coingecko_markets,
        coingecko_coin_detail,
        # Historical
        coingecko_market_chart,
        coingecko_market_chart_range,
        coingecko_ohlc,
        # On-chain
        coingecko_token_price_onchain,
        # Enumeration
        enumerate_coingecko,
    ]
)


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
