"""Declarative output schemas for the CoinGecko connectors.

One :class:`OutputSpec` per connector that projects a shaped DataFrame
out of CoinGecko's raw JSON responses. Columns declared here are the
contract with the MCP tool catalog — renaming or re-ordering them is a
breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="thumb", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

TRENDING_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="score", role=ColumnRole.DATA),
    ]
)

GAINERS_LOSERS_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="direction", role=ColumnRole.METADATA),
        Column(name="usd_price_percent_change"),
    ]
)

PRICE_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
    ]
)

MARKETS_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="coingecko_coin"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="market_cap_rank", role=ColumnRole.METADATA),
        Column(name="current_price"),
        Column(name="market_cap"),
        Column(name="total_volume"),
        Column(name="high_24h"),
        Column(name="low_24h"),
        Column(name="price_change_percentage_24h"),
        Column(name="ath"),
        Column(name="atl"),
        Column(name="circulating_supply"),
        Column(name="total_supply"),
        Column(name="last_updated", role=ColumnRole.METADATA),
    ]
)

MARKET_CHART_OUTPUT = OutputSpec(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY),
        Column(name="price"),
        Column(name="market_cap"),
        Column(name="total_volume"),
    ]
)

OHLC_OUTPUT = OutputSpec(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
    ]
)

ONCHAIN_PRICE_OUTPUT = OutputSpec(
    columns=[
        Column(name="contract_address", role=ColumnRole.KEY, namespace="coingecko_onchain"),
        Column(name="price_usd"),
    ]
)

ENUMERATE_OUTPUT = OutputSpec(
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
