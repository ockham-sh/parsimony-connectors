"""Pydantic parameter models for the CoinGecko connectors.

Every ``@connector`` / ``@enumerator`` function in ``__init__.py`` accepts
one of the classes defined here as its typed ``params`` argument. These
classes form part of the public import surface — tests and downstream
callers depend on them.

Path-component validators are co-located because several connectors
interpolate user-supplied values (coin id, blockchain network,
contract address) into the request path. The validators reject anything
that isn't URL-safe before the HTTP layer sees it.
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_PATH_SAFE_RE = re.compile(r"^[a-zA-Z0-9._\-]+$")
_NETWORK_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")
_CONTRACT_ADDR_RE = re.compile(r"^[a-zA-Z0-9x,]+$")


class CoinGeckoSearchParams(BaseModel):
    """Search CoinGecko for coins, exchanges, or NFTs by name or symbol."""

    query: str = Field(..., min_length=1, description="Search term, e.g. 'solana' or 'SOL'")


class CoinGeckoTrendingParams(BaseModel):
    """No parameters — trending is always the last 24 hours."""

    pass


class CoinGeckoTopMoversParams(BaseModel):
    """Parameters for top gainers and losers."""

    vs_currency: str = Field(
        default="usd",
        description="Target currency for price data, e.g. usd, eur, btc",
    )
    duration: Literal["1h", "24h", "7d", "14d", "30d", "60d", "1y"] = Field(
        default="24h",
        description="Time window: 1h, 24h, 7d, 14d, 30d, 60d, or 1y",
    )
    top_coins: Literal["300", "1000"] = Field(
        default="1000",
        description="Pool size to rank from: 300 or 1000 top coins by market cap",
    )


class CoinGeckoPriceParams(BaseModel):
    """Parameters for simple price lookup."""

    ids: str = Field(
        ...,
        description="Comma-separated CoinGecko coin IDs, e.g. 'bitcoin,ethereum'. Use coingecko_search to resolve IDs.",
    )
    vs_currencies: str = Field(
        default="usd",
        description="Comma-separated target currencies, e.g. 'usd,eur,btc'",
    )
    include_market_cap: bool = Field(default=True, description="Include market cap values")
    include_24hr_vol: bool = Field(default=True, description="Include 24h trading volume")
    include_24hr_change: bool = Field(default=True, description="Include 24h price change percentage")


class CoinGeckoMarketsParams(BaseModel):
    """Parameters for paginated coin market listings."""

    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    ids: str | None = Field(
        default=None,
        description="Comma-separated coin IDs to filter, e.g. 'bitcoin,ethereum'. Omit for top N by market cap.",
    )
    order: Literal[
        "market_cap_desc",
        "market_cap_asc",
        "volume_desc",
        "volume_asc",
        "id_desc",
        "id_asc",
    ] = Field(default="market_cap_desc", description="Sort order")
    per_page: int = Field(default=100, ge=1, le=250, description="Results per page (max 250)")
    page: int = Field(default=1, ge=1, description="Page number")
    sparkline: bool = Field(default=False, description="Include 7-day sparkline data")


class CoinGeckoCoinDetailParams(BaseModel):
    """Parameters for fetching full coin metadata."""

    coin_id: Annotated[str, "ns:coingecko_coin"] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    localization: bool = Field(default=False, description="Include localized language data (inflates response)")
    tickers: bool = Field(default=False, description="Include exchange ticker data")
    market_data: bool = Field(default=True, description="Include current market data")
    community_data: bool = Field(default=False, description="Include community stats")
    developer_data: bool = Field(default=False, description="Include developer/GitHub stats")

    @field_validator("coin_id")
    @classmethod
    def _path_safe_coin_id(cls, v: str) -> str:
        if not _PATH_SAFE_RE.match(v):
            raise ValueError(f"coin_id contains unsafe characters for URL path: {v!r}")
        return v


class CoinGeckoMarketChartParams(BaseModel):
    """Parameters for historical price chart by number of days."""

    coin_id: Annotated[str, "ns:coingecko_coin"] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    days: str = Field(
        ...,
        description=(
            "Number of days of data: integer (e.g. '30') or 'max' for full history. "
            "Auto-granularity: 1d→5-min, 2-90d→hourly, 90d+→daily. "
            "Override with interval= parameter."
        ),
    )
    interval: Literal["5m", "hourly", "daily"] | None = Field(
        default=None,
        description="Force data interval: '5m', 'hourly', or 'daily'. None = auto.",
    )

    @field_validator("coin_id")
    @classmethod
    def _path_safe_coin_id(cls, v: str) -> str:
        if not _PATH_SAFE_RE.match(v):
            raise ValueError(f"coin_id contains unsafe characters for URL path: {v!r}")
        return v


class CoinGeckoMarketChartRangeParams(BaseModel):
    """Parameters for historical price chart between two dates."""

    model_config = ConfigDict(populate_by_name=True)

    coin_id: Annotated[str, "ns:coingecko_coin"] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    from_date: str = Field(
        ...,
        alias="from",
        description="Start date ISO 8601, e.g. '2024-01-01'. Use as from_date='2024-01-01'",
    )
    to_date: str = Field(
        ...,
        alias="to",
        description="End date ISO 8601, e.g. '2024-12-31'. Use as to_date='2024-12-31'",
    )

    @field_validator("coin_id")
    @classmethod
    def _path_safe_coin_id(cls, v: str) -> str:
        if not _PATH_SAFE_RE.match(v):
            raise ValueError(f"coin_id contains unsafe characters for URL path: {v!r}")
        return v


class CoinGeckoOhlcParams(BaseModel):
    """Parameters for OHLC candlestick data."""

    coin_id: Annotated[str, "ns:coingecko_coin"] = Field(
        ..., description="CoinGecko coin ID, e.g. 'bitcoin'. Use coingecko_search to resolve."
    )
    vs_currency: str = Field(default="usd", description="Target currency, e.g. usd, eur, btc")
    days: Literal[1, 7, 14, 30, 90, 180, 365] = Field(
        default=30, description="Candle range in days: 1, 7, 14, 30, 90, 180, or 365"
    )

    @field_validator("coin_id")
    @classmethod
    def _path_safe_coin_id(cls, v: str) -> str:
        if not _PATH_SAFE_RE.match(v):
            raise ValueError(f"coin_id contains unsafe characters for URL path: {v!r}")
        return v


class CoinGeckoTokenPriceOnchainParams(BaseModel):
    """Parameters for on-chain token price lookup via GeckoTerminal."""

    network: str = Field(
        ...,
        description=(
            "Blockchain network ID, e.g. 'eth' (Ethereum), 'bsc' (BNB Chain), "
            "'polygon-pos', 'arbitrum-one', 'solana'. Use lowercase with hyphens."
        ),
    )
    contract_addresses: str = Field(
        ...,
        description=(
            "Comma-separated token contract addresses (checksum or lowercase),"
            " e.g. '0xdac17f958d2ee523a2206206994597c13d831ec7'"
        ),
    )
    vs_currencies: str = Field(
        default="usd",
        description="Comma-separated target currencies for price. Only 'usd' is reliably available.",
    )

    @field_validator("network")
    @classmethod
    def _path_safe_network(cls, v: str) -> str:
        if not _NETWORK_RE.match(v):
            raise ValueError(f"network contains unsafe characters for URL path: {v!r}")
        return v

    @field_validator("contract_addresses")
    @classmethod
    def _path_safe_addresses(cls, v: str) -> str:
        if not _CONTRACT_ADDR_RE.match(v):
            raise ValueError(f"contract_addresses contains unsafe characters for URL path: {v!r}")
        return v


class CoinGeckoEnumerateParams(BaseModel):
    """No parameters — enumerates the full CoinGecko coin catalog (~15 000 entries)."""

    include_platform: bool = Field(
        default=False,
        description="Include contract address platforms (significantly increases response size)",
    )


__all__ = [
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
]
