"""Declarative output schemas for the Tiingo connectors.

One :class:`OutputSpec` per connector that projects a shaped DataFrame
out of Tiingo's raw JSON. Columns declared here are the contract with
the MCP tool catalog — renaming or re-ordering them is a breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="asset_type", role=ColumnRole.METADATA),
        Column(name="is_active", role=ColumnRole.METADATA),
        Column(name="country_code", role=ColumnRole.METADATA),
        Column(name="perma_ticker", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="open_figi", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

# Company metadata for one ticker — a one-row frame (the raw endpoint returns a
# single JSON record).
META_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="startDate", role=ColumnRole.METADATA),
        Column(name="endDate", role=ColumnRole.METADATA),
        Column(name="exchangeCode", role=ColumnRole.METADATA),
    ]
)

# Fundamentals reference metadata — one row per ticker (the raw endpoint returns
# a JSON array). All fields are reference attributes, so none is DATA.
FUNDAMENTALS_META_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="sector", role=ColumnRole.METADATA),
        Column(name="industry", role=ColumnRole.METADATA),
        Column(name="sicCode", role=ColumnRole.METADATA),
        Column(name="sicSector", role=ColumnRole.METADATA),
        Column(name="sicIndustry", role=ColumnRole.METADATA),
        Column(name="reportingCurrency", role=ColumnRole.METADATA),
        Column(name="location", role=ColumnRole.METADATA),
        Column(name="companyWebsite", role=ColumnRole.METADATA),
        Column(name="secFilingWebsite", role=ColumnRole.METADATA),
        Column(name="isActive", role=ColumnRole.METADATA),
        Column(name="isADR", role=ColumnRole.METADATA),
        Column(name="statementLastUpdated", role=ColumnRole.METADATA),
        Column(name="dailyLastUpdated", role=ColumnRole.METADATA),
        Column(name="permaTicker", role=ColumnRole.METADATA, exclude_from_llm_view=True),
        Column(name="dataProviderPermaTicker", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

EOD_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="close"),
        Column(name="high"),
        Column(name="low"),
        Column(name="open"),
        Column(name="volume"),
        Column(name="adj_close"),
        Column(name="adj_high"),
        Column(name="adj_low"),
        Column(name="adj_open"),
        Column(name="adj_volume"),
        Column(name="div_cash"),
        Column(name="split_factor"),
    ]
)

IEX_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="timestamp", role=ColumnRole.METADATA),
        Column(name="tngo_last"),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="volume"),
        Column(name="prev_close"),
        Column(name="mid"),
        Column(name="bid_price"),
        Column(name="ask_price"),
        Column(name="bid_size"),
        Column(name="ask_size"),
    ]
)

IEX_HIST_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
    ]
)

DEFINITIONS_OUTPUT = OutputSpec(
    columns=[
        Column(name="data_code", role=ColumnRole.KEY, namespace="tiingo_data_code"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="statement_type", role=ColumnRole.METADATA),
        Column(name="units", role=ColumnRole.METADATA),
    ]
)

NEWS_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="published_date", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="tickers", role=ColumnRole.METADATA),
        Column(name="tags", role=ColumnRole.METADATA),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="url", role=ColumnRole.METADATA, exclude_from_llm_view=True),
    ]
)

CRYPTO_PRICES_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_crypto"),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="volume"),
        Column(name="volume_notional"),
        Column(name="trades_done"),
    ]
)

CRYPTO_TOP_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_crypto"),
        Column(name="last_price"),
        Column(name="quote_timestamp", role=ColumnRole.METADATA),
        Column(name="bid_price"),
        Column(name="ask_price"),
        Column(name="bid_size"),
        Column(name="ask_size"),
        Column(name="last_size_notional"),
        Column(name="last_exchange", role=ColumnRole.METADATA),
    ]
)

FX_PRICES_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_fx"),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
    ]
)

FX_TOP_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_fx"),
        Column(name="quote_timestamp", role=ColumnRole.METADATA),
        Column(name="mid_price"),
        Column(name="bid_price"),
        Column(name="ask_price"),
        Column(name="bid_size"),
        Column(name="ask_size"),
    ]
)

# The supported_tickers.csv snapshot header is
# ``ticker, exchange, assetType, priceCurrency, startDate, endDate`` — there is
# no name or country column, so ``name`` (TITLE) falls back to the ticker
# symbol and only columns the CSV actually carries are declared (enumerators
# require an EXACT column match, and a column that is always empty is dead
# metadata).
ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="ticker", role=ColumnRole.KEY, namespace="tiingo_ticker"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="asset_type", role=ColumnRole.METADATA),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="price_currency", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
    ]
)


__all__ = [
    "CRYPTO_PRICES_OUTPUT",
    "CRYPTO_TOP_OUTPUT",
    "DEFINITIONS_OUTPUT",
    "ENUMERATE_OUTPUT",
    "EOD_OUTPUT",
    "FX_PRICES_OUTPUT",
    "FX_TOP_OUTPUT",
    "IEX_HIST_OUTPUT",
    "IEX_OUTPUT",
    "NEWS_OUTPUT",
    "SEARCH_OUTPUT",
]
