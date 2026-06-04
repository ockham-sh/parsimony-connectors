"""Declarative output schemas for the Alpha Vantage connectors.

One :class:`OutputConfig` per connector that projects a shaped DataFrame
out of Alpha Vantage's raw JSON / CSV. Columns declared here are the
contract with the MCP tool catalog — renaming or re-ordering them is a
breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="region", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="matchScore", role=ColumnRole.METADATA),
    ]
)

QUOTE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="price", dtype="numeric"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="latest_trading_day", role=ColumnRole.METADATA, dtype="date"),
        Column(name="previous_close", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="change_percent", dtype="numeric"),
    ]
)

DAILY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)

EARNINGS_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="fiscalDateEnding", dtype="date", role=ColumnRole.DATA),
        Column(name="reportedDate", dtype="date", role=ColumnRole.DATA),
        Column(name="reportedEPS", dtype="numeric", role=ColumnRole.DATA),
        Column(name="estimatedEPS", dtype="numeric", role=ColumnRole.DATA),
        Column(name="surprise", dtype="numeric", role=ColumnRole.DATA),
        Column(name="surprisePercentage", dtype="numeric", role=ColumnRole.DATA),
        Column(name="reportTime", role=ColumnRole.METADATA),
    ]
)

FX_RATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="from_currency", role=ColumnRole.KEY, namespace="alpha_vantage_fx"),
        Column(name="from_currency_name", role=ColumnRole.TITLE),
        Column(name="to_currency", role=ColumnRole.METADATA),
        Column(name="to_currency_name", role=ColumnRole.METADATA),
        Column(name="exchange_rate", dtype="numeric"),
        Column(name="bid_price", dtype="numeric"),
        Column(name="ask_price", dtype="numeric"),
        Column(name="last_refreshed", role=ColumnRole.METADATA),
    ]
)

FX_DAILY_OUTPUT = OutputConfig(
    columns=[
        # The entity is the currency pair (e.g. "EUR/USD"). The connector body
        # injects a ``pair`` column from the from_symbol/to_symbol params — the
        # raw payload carries no pair field, so the KEY must be synthesised.
        Column(
            name="pair",
            role=ColumnRole.KEY,
            namespace="alpha_vantage_fx",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
    ]
)

CRYPTO_DAILY_OUTPUT = OutputConfig(
    columns=[
        # The crypto rows carry no symbol field; the connector body injects a
        # ``symbol`` column from the param so the KEY is populated.
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            namespace="alpha_vantage_crypto",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)

ECON_OUTPUT = OutputConfig(
    columns=[
        Column(name="name", role=ColumnRole.KEY, namespace="alpha_vantage_econ"),
        Column(name="series_name", role=ColumnRole.TITLE),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="interval", role=ColumnRole.METADATA),
    ]
)

NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="url", role=ColumnRole.KEY, namespace="alpha_vantage_news"),
        Column(name="time_published", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="overall_sentiment_score", dtype="numeric", role=ColumnRole.DATA),
        Column(name="overall_sentiment_label", role=ColumnRole.METADATA),
        Column(name="summary", role=ColumnRole.METADATA),
        Column(name="banner_image", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

MOVERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="category", role=ColumnRole.TITLE),
        Column(name="price", dtype="numeric", role=ColumnRole.DATA),
        Column(name="change_amount", dtype="numeric", role=ColumnRole.DATA),
        Column(name="change_percentage", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)

OPTIONS_OUTPUT = OutputConfig(
    columns=[
        Column(name="contractID", role=ColumnRole.KEY, namespace="alpha_vantage_options"),
        Column(name="symbol", role=ColumnRole.TITLE),
        Column(name="expiration", dtype="date", role=ColumnRole.DATA),
        Column(name="strike", dtype="numeric", role=ColumnRole.DATA),
        Column(name="type", role=ColumnRole.METADATA),
        Column(name="last", dtype="numeric", role=ColumnRole.DATA),
        Column(name="bid", dtype="numeric", role=ColumnRole.DATA),
        Column(name="ask", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
        Column(name="open_interest", dtype="numeric", role=ColumnRole.DATA),
        Column(name="implied_volatility", dtype="numeric", role=ColumnRole.DATA),
        Column(name="delta", dtype="numeric", role=ColumnRole.DATA),
        Column(name="gamma", dtype="numeric", role=ColumnRole.DATA),
        Column(name="theta", dtype="numeric", role=ColumnRole.DATA),
        Column(name="vega", dtype="numeric", role=ColumnRole.DATA),
    ]
)

INTRADAY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        Column(name="timestamp", dtype="datetime", role=ColumnRole.DATA),
        Column(name="open", dtype="numeric", role=ColumnRole.DATA),
        Column(name="high", dtype="numeric", role=ColumnRole.DATA),
        Column(name="low", dtype="numeric", role=ColumnRole.DATA),
        Column(name="close", dtype="numeric", role=ColumnRole.DATA),
        Column(name="volume", dtype="numeric", role=ColumnRole.DATA),
    ]
)

EARNINGS_CAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="reportDate", dtype="date", role=ColumnRole.DATA),
        Column(name="fiscalDateEnding", dtype="date", role=ColumnRole.DATA),
        Column(name="estimate", dtype="numeric", role=ColumnRole.DATA),
        Column(name="currency", role=ColumnRole.METADATA),
    ]
)

IPO_CAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="ipoDate", dtype="date", role=ColumnRole.DATA),
        Column(name="priceRangeLow", dtype="numeric", role=ColumnRole.DATA),
        Column(name="priceRangeHigh", dtype="numeric", role=ColumnRole.DATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
    ]
)

TECHNICAL_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage",
        ),
        # `datetime` (not `date`) so intraday intervals (1min..60min) keep their
        # time component. `date` runs `dt.normalize()`, which would zero out the
        # time on every row regardless of interval.
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    ]
)

METAL_SPOT_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage_metal"),
        Column(name="nominal", role=ColumnRole.TITLE),
        Column(name="price", dtype="numeric", role=ColumnRole.DATA),
        Column(name="timestamp", role=ColumnRole.METADATA),
    ]
)

METAL_HISTORY_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="symbol",
            role=ColumnRole.KEY,
            param_key="symbol",
            namespace="alpha_vantage_metal",
        ),
        Column(name="date", dtype="date", role=ColumnRole.DATA),
        Column(name="price", dtype="numeric", role=ColumnRole.DATA),
    ]
)

LISTING_OUTPUT = OutputConfig(
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

OVERVIEW_OUTPUT = OutputConfig(
    columns=[
        Column(name="Symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="Name", role=ColumnRole.TITLE),
        # All remaining ~50 overview fields fold in as DATA (string passthrough).
    ]
)

STATEMENT_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="fiscalDateEnding", role=ColumnRole.TITLE),
        # All remaining statement line items fold in as DATA (string passthrough).
    ]
)

ETF_PROFILE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace="alpha_vantage"),
        Column(name="holding_symbol", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="weight", dtype="numeric", role=ColumnRole.DATA),
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
