"""Declarative output schemas for the Finnhub connectors.

One :class:`OutputConfig` per connector that projects a shaped DataFrame
out of Finnhub's raw JSON. Columns declared here are the contract with
the MCP tool catalog — renaming or re-ordering them is a breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="description", role=ColumnRole.TITLE),
        Column(name="display_symbol", role=ColumnRole.METADATA),
        Column(name="type", role=ColumnRole.METADATA),
    ]
)

QUOTE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="current_price", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="change_percent", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="open", dtype="numeric"),
        Column(name="prev_close", dtype="numeric"),
        Column(name="timestamp", role=ColumnRole.METADATA, dtype="timestamp"),
    ]
)

PEERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
    ]
)

RECOMMENDATION_OUTPUT = OutputConfig(
    columns=[
        Column(name="period", role=ColumnRole.KEY, dtype="date"),
        Column(name="strong_buy", dtype="numeric"),
        Column(name="buy", dtype="numeric"),
        Column(name="hold", dtype="numeric"),
        Column(name="sell", dtype="numeric"),
        Column(name="strong_sell", dtype="numeric"),
    ]
)

EARNINGS_OUTPUT = OutputConfig(
    columns=[
        Column(name="period", role=ColumnRole.KEY, dtype="date"),
        Column(name="quarter", role=ColumnRole.METADATA),
        Column(name="year", role=ColumnRole.METADATA),
        Column(name="eps_actual", dtype="numeric"),
        Column(name="eps_estimate", dtype="numeric"),
        Column(name="eps_surprise", dtype="numeric"),
        Column(name="eps_surprise_percent", dtype="numeric"),
    ]
)

NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="datetime", role=ColumnRole.METADATA, dtype="timestamp"),
        Column(name="headline", role=ColumnRole.TITLE),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="related", role=ColumnRole.METADATA),
        Column(name="summary", role=ColumnRole.METADATA),
        Column(name="url", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="image", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

EARNINGS_CAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="date", role=ColumnRole.METADATA, dtype="date"),
        Column(name="year", role=ColumnRole.METADATA),
        Column(name="quarter", role=ColumnRole.METADATA),
        Column(name="hour", role=ColumnRole.METADATA),
        Column(name="eps_estimate", dtype="numeric"),
        Column(name="eps_actual", dtype="numeric"),
        Column(name="revenue_estimate", dtype="numeric"),
        Column(name="revenue_actual", dtype="numeric"),
    ]
)

IPO_CAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.METADATA, dtype="date"),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="status", role=ColumnRole.METADATA),
        Column(name="price", dtype="numeric"),
        Column(name="number_of_shares", dtype="numeric"),
        Column(name="total_shares_value", dtype="numeric"),
    ]
)

ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="description", role=ColumnRole.TITLE),
        Column(name="display_symbol", role=ColumnRole.METADATA),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="mic", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="isin", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)


__all__ = [
    "EARNINGS_CAL_OUTPUT",
    "EARNINGS_OUTPUT",
    "ENUMERATE_OUTPUT",
    "IPO_CAL_OUTPUT",
    "NEWS_OUTPUT",
    "PEERS_OUTPUT",
    "QUOTE_OUTPUT",
    "RECOMMENDATION_OUTPUT",
    "SEARCH_OUTPUT",
]
