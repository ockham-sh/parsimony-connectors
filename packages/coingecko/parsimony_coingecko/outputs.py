"""Declarative output schemas for the CoinGecko connectors.

One :class:`OutputConfig` per connector that projects a shaped DataFrame
out of CoinGecko's raw JSON responses. Columns declared here are the
contract with the MCP tool catalog — renaming or re-ordering them is a
breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="thumb", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

TRENDING_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="score", role=ColumnRole.METADATA),
    ]
)

GAINERS_LOSERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="direction", role=ColumnRole.METADATA),
        Column(name="usd_price_percent_change", dtype="numeric"),
    ]
)

PRICE_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
    ]
)

MARKETS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="current_price", dtype="numeric"),
        Column(name="market_cap", dtype="numeric"),
        Column(name="total_volume", dtype="numeric"),
        Column(name="high_24h", dtype="numeric"),
        Column(name="low_24h", dtype="numeric"),
        Column(name="price_change_percentage_24h", dtype="numeric"),
        Column(name="ath", dtype="numeric"),
        Column(name="atl", dtype="numeric"),
        Column(name="circulating_supply", dtype="numeric"),
        Column(name="total_supply", dtype="numeric"),
        Column(name="last_updated", role=ColumnRole.METADATA),
    ]
)

MARKET_CHART_OUTPUT = OutputConfig(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY, dtype="timestamp"),
        Column(name="price", dtype="numeric"),
        Column(name="market_cap", dtype="numeric"),
        Column(name="total_volume", dtype="numeric"),
    ]
)

OHLC_OUTPUT = OutputConfig(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY, dtype="timestamp"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
    ]
)

ONCHAIN_PRICE_OUTPUT = OutputConfig(
    columns=[
        Column(name="contract_address", role=ColumnRole.KEY, namespace="coingecko_onchain"),
        Column(name="price_usd", dtype="numeric"),
    ]
)

ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="platforms", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)


__all__ = [
    "ENUMERATE_OUTPUT",
    "GAINERS_LOSERS_OUTPUT",
    "MARKETS_OUTPUT",
    "MARKET_CHART_OUTPUT",
    "OHLC_OUTPUT",
    "ONCHAIN_PRICE_OUTPUT",
    "PRICE_OUTPUT",
    "SEARCH_OUTPUT",
    "TRENDING_OUTPUT",
]
