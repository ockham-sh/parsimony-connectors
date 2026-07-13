"""Declarative output schemas for the Alpha Vantage connectors.

One :class:`OutputSpec` per connector that projects a shaped DataFrame
out of Alpha Vantage's raw JSON / CSV. Columns declared here are the
contract with the MCP tool catalog — renaming or re-ordering them is a
breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="region", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="matchScore", role=ColumnRole.METADATA),
    ]
)

QUOTE_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="price"),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="volume"),
        Column(name="latest_trading_day", role=ColumnRole.METADATA),
        Column(name="previous_close"),
        Column(name="change"),
        Column(name="change_percent"),
    ]
)

DAILY_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage",
        ),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="open", role=ColumnRole.DATA),
        Column(name="high", role=ColumnRole.DATA),
        Column(name="low", role=ColumnRole.DATA),
        Column(name="close", role=ColumnRole.DATA),
        Column(name="volume", role=ColumnRole.DATA),
    ]
)

EARNINGS_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage",
        ),
        Column(name="fiscalDateEnding", role=ColumnRole.DATA),
        Column(name="reportedDate", role=ColumnRole.DATA),
        Column(name="reportedEPS", role=ColumnRole.DATA),
        Column(name="estimatedEPS", role=ColumnRole.DATA),
        Column(name="surprise", role=ColumnRole.DATA),
        Column(name="surprisePercentage", role=ColumnRole.DATA),
        Column(name="reportTime", role=ColumnRole.METADATA),
    ]
)

FX_RATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="from_currency", role=ColumnRole.KEY, namespace="alpha_vantage_fx"),
        Column(name="from_currency_name", role=ColumnRole.TITLE),
        Column(name="to_currency", role=ColumnRole.METADATA),
        Column(name="to_currency_name", role=ColumnRole.METADATA),
        Column(name="exchange_rate"),
        Column(name="bid_price"),
        Column(name="ask_price"),
        Column(name="last_refreshed", role=ColumnRole.METADATA),
    ]
)

FX_DAILY_OUTPUT = OutputSpec(
    columns=[
        # The entity is the currency pair (e.g. "EUR/USD"). The connector body
        # injects a ``pair`` column from the from_symbol/to_symbol params — the
        # raw payload carries no pair field, so the KEY must be synthesised.
        Column(
            name="pair",
            role=ColumnRole.KEY,
            namespace="alpha_vantage_fx",
        ),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="open", role=ColumnRole.DATA),
        Column(name="high", role=ColumnRole.DATA),
        Column(name="low", role=ColumnRole.DATA),
        Column(name="close", role=ColumnRole.DATA),
    ]
)

CRYPTO_DAILY_OUTPUT = OutputSpec(
    columns=[
        # The crypto rows carry no symbol field; the connector body injects a
        # ``symbol`` column from the param so the KEY is populated.
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage_crypto",
        ),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="open", role=ColumnRole.DATA),
        Column(name="high", role=ColumnRole.DATA),
        Column(name="low", role=ColumnRole.DATA),
        Column(name="close", role=ColumnRole.DATA),
        Column(name="volume", role=ColumnRole.DATA),
    ]
)

ECON_OUTPUT = OutputSpec(
    columns=[
        Column(name="name", role=ColumnRole.KEY, namespace="alpha_vantage_econ"),
        Column(name="series_name", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="interval", role=ColumnRole.METADATA),
    ]
)

NEWS_OUTPUT = OutputSpec(
    columns=[
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="url", role=ColumnRole.KEY, namespace="alpha_vantage_news"),
        Column(name="time_published", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="overall_sentiment_score", role=ColumnRole.DATA),
        Column(name="overall_sentiment_label", role=ColumnRole.METADATA),
        Column(name="summary", role=ColumnRole.METADATA),
        Column(name="banner_image", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

MOVERS_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="category", role=ColumnRole.TITLE),
        Column(name="price", role=ColumnRole.DATA),
        Column(name="change_amount", role=ColumnRole.DATA),
        Column(name="change_percentage", role=ColumnRole.DATA),
        Column(name="volume", role=ColumnRole.DATA),
    ]
)

OPTIONS_OUTPUT = OutputSpec(
    columns=[
        Column(name="contractID", role=ColumnRole.KEY, namespace="alpha_vantage_options"),
        Column(name="symbol", role=ColumnRole.TITLE),
        Column(name="expiration", role=ColumnRole.DATA),
        Column(name="strike", role=ColumnRole.DATA),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="last", role=ColumnRole.DATA),
        Column(name="bid", role=ColumnRole.DATA),
        Column(name="ask", role=ColumnRole.DATA),
        Column(name="volume", role=ColumnRole.DATA),
        Column(name="open_interest", role=ColumnRole.DATA),
        Column(name="implied_volatility", role=ColumnRole.DATA),
        Column(name="delta", role=ColumnRole.DATA),
        Column(name="gamma", role=ColumnRole.DATA),
        Column(name="theta", role=ColumnRole.DATA),
        Column(name="vega", role=ColumnRole.DATA),
    ]
)

INTRADAY_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage",
        ),
        Column(name="timestamp", role=ColumnRole.DATA),
        Column(name="open", role=ColumnRole.DATA),
        Column(name="high", role=ColumnRole.DATA),
        Column(name="low", role=ColumnRole.DATA),
        Column(name="close", role=ColumnRole.DATA),
        Column(name="volume", role=ColumnRole.DATA),
    ]
)

EARNINGS_CAL_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="reportDate", role=ColumnRole.DATA),
        Column(name="fiscalDateEnding", role=ColumnRole.DATA),
        Column(name="estimate", role=ColumnRole.DATA),
        Column(name="currency", role=ColumnRole.METADATA),
    ]
)

IPO_CAL_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="ipoDate", role=ColumnRole.DATA),
        Column(name="priceRangeLow", role=ColumnRole.DATA),
        Column(name="priceRangeHigh", role=ColumnRole.DATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
    ]
)

TECHNICAL_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage",
        ),
        # `datetime` (not `date`) so intraday intervals (1min..60min) keep their
        # time component. `date` runs `dt.normalize()`, which would zero out the
        # time on every row regardless of interval.
        Column(name="date", role=ColumnRole.DATA),
    ]
)

METAL_SPOT_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage_metal"),
        Column(name="nominal", role=ColumnRole.TITLE),
        Column(name="price", role=ColumnRole.DATA),
        Column(name="timestamp", role=ColumnRole.METADATA),
    ]
)

METAL_HISTORY_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage_metal",
        ),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="price", role=ColumnRole.DATA),
    ]
)

LISTING_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="assetType", role=ColumnRole.METADATA),
        Column(name="ipoDate", role=ColumnRole.METADATA),
        Column(name="status", role=ColumnRole.METADATA),
    ]
)

# --- Fundamentals: single-row / period-row shapes ------------------------------
#
# OVERVIEW is a flat ~50-field dict; the body emits a single row keyed by
# ``symbol`` with ``Name`` as the title and every remaining provider field folded
# in as a DATA column (no numeric coercion — many fields are "None"/"-" strings
# that would coerce to all-NaN and raise). INCOME/BALANCE/CASH_FLOW return one
# row per reporting period (keyed by ``symbol`` with the period's
# ``fiscalDateEnding`` as a DATA column); ETF_PROFILE returns one row per holding.

OVERVIEW_OUTPUT = OutputSpec(
    columns=[
        Column(name="Symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="Name", role=ColumnRole.TITLE),
        # All remaining ~50 overview fields fold in as DATA (string passthrough).
    ]
)

STATEMENT_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="fiscalDateEnding", role=ColumnRole.TITLE),
        # All remaining statement line items fold in as DATA (string passthrough).
    ]
)

ETF_PROFILE_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="holding_symbol", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="weight", role=ColumnRole.DATA),
    ]
)


__all__ = [
    "CRYPTO_DAILY_OUTPUT",
    "DAILY_OUTPUT",
    "EARNINGS_CAL_OUTPUT",
    "EARNINGS_OUTPUT",
    "ECON_OUTPUT",
    "ETF_PROFILE_OUTPUT",
    "FX_DAILY_OUTPUT",
    "FX_RATE_OUTPUT",
    "INTRADAY_OUTPUT",
    "IPO_CAL_OUTPUT",
    "LISTING_OUTPUT",
    "METAL_HISTORY_OUTPUT",
    "METAL_SPOT_OUTPUT",
    "MOVERS_OUTPUT",
    "NEWS_OUTPUT",
    "OPTIONS_OUTPUT",
    "OVERVIEW_OUTPUT",
    "QUOTE_OUTPUT",
    "SEARCH_OUTPUT",
    "STATEMENT_OUTPUT",
    "TECHNICAL_OUTPUT",
]
