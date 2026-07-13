"""Declarative output schemas for the Finnhub connectors.

One :class:`OutputSpec` per connector that projects a shaped DataFrame
out of Finnhub's raw JSON. Columns declared here are the contract with
the MCP tool catalog — renaming or re-ordering them is a breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="description", role=ColumnRole.TITLE),
        Column(name="display_symbol", role=ColumnRole.METADATA),
        Column(name="type", role=ColumnRole.METADATA),
    ]
)

# Company profile for one symbol — a one-row frame (the raw endpoint returns a
# single JSON record). The three share/cap figures are the quantitative DATA.
PROFILE_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="marketCapitalization"),
        Column(name="shareOutstanding"),
        Column(name="floatingShare"),
        Column(name="country", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="estimateCurrency", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="ipo", role=ColumnRole.METADATA),
        Column(name="finnhubIndustry", role=ColumnRole.METADATA),
        Column(name="phone", role=ColumnRole.METADATA),
        Column(name="weburl", role=ColumnRole.METADATA),
        Column(name="logo", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

QUOTE_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="current_price"),
        Column(name="change"),
        Column(name="change_percent"),
        Column(name="high"),
        Column(name="low"),
        Column(name="open"),
        Column(name="prev_close"),
        Column(name="timestamp", role=ColumnRole.METADATA),
    ]
)

PEERS_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
    ]
)

RECOMMENDATION_OUTPUT = OutputSpec(
    columns=[
        Column(name="period", role=ColumnRole.KEY),
        Column(name="strong_buy"),
        Column(name="buy"),
        Column(name="hold"),
        Column(name="sell"),
        Column(name="strong_sell"),
    ]
)

EARNINGS_OUTPUT = OutputSpec(
    columns=[
        Column(name="period", role=ColumnRole.KEY),
        Column(name="quarter", role=ColumnRole.METADATA),
        Column(name="year", role=ColumnRole.METADATA),
        Column(name="eps_actual"),
        Column(name="eps_estimate"),
        Column(name="eps_surprise"),
        Column(name="eps_surprise_percent"),
    ]
)

NEWS_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="datetime", role=ColumnRole.METADATA),
        Column(name="headline", role=ColumnRole.TITLE),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="related", role=ColumnRole.METADATA),
        Column(name="summary", role=ColumnRole.METADATA),
        Column(name="url", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="image", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

EARNINGS_CAL_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="date", role=ColumnRole.METADATA),
        Column(name="year", role=ColumnRole.METADATA),
        Column(name="quarter", role=ColumnRole.METADATA),
        Column(name="hour", role=ColumnRole.METADATA),
        Column(name="eps_estimate"),
        Column(name="eps_actual"),
        Column(name="revenue_estimate"),
        Column(name="revenue_actual"),
    ]
)

IPO_CAL_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="finnhub_symbol"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="status", role=ColumnRole.METADATA),
        # Finnhub reports IPO price as a string that is sometimes a single value
        # ("18.00") and sometimes a range ("18.00-20.00"). Coercing to numeric
        # silently nulls every range, so keep the verbatim string as metadata.
        Column(name="price_range", role=ColumnRole.METADATA),
        Column(name="number_of_shares"),
        Column(name="total_shares_value"),
    ]
)

ENUMERATE_OUTPUT = OutputSpec(
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
