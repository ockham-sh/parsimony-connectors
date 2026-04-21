"""Declarative output schemas for the EODHD connectors.

One :class:`OutputConfig` per connector that projects a shaped DataFrame
out of EODHD's raw JSON. Columns declared here are the contract with the
MCP tool catalog — renaming or re-ordering them is a breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

EOD_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="adjusted_close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
    ]
)

LIVE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="timestamp", role=ColumnRole.METADATA, dtype="timestamp"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="previousClose", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="change_p", dtype="numeric"),
    ]
)

INTRADAY_OUTPUT = OutputConfig(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY, dtype="timestamp"),
        Column(name="datetime", role=ColumnRole.METADATA),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
    ]
)

BULK_EOD_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange_short_name", role=ColumnRole.METADATA),
        Column(name="date", dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="adjusted_close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
    ]
)

DIVIDENDS_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="declarationDate", dtype="date"),
        Column(name="recordDate", dtype="date"),
        Column(name="paymentDate", dtype="date"),
        Column(name="period", role=ColumnRole.METADATA),
        Column(name="value", dtype="numeric"),
        Column(name="unadjustedValue", dtype="numeric"),
        Column(name="currency", role=ColumnRole.METADATA),
    ]
)

SPLITS_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="split", dtype="auto"),
    ]
)

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="Code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="Exchange", role=ColumnRole.METADATA),
        Column(name="Type", role=ColumnRole.METADATA),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="ISIN", role=ColumnRole.METADATA),
    ]
)

EXCHANGES_OUTPUT = OutputConfig(
    columns=[
        Column(name="Code", role=ColumnRole.KEY),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="OperatingMIC", role=ColumnRole.METADATA),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="CountryISO2", role=ColumnRole.METADATA),
        Column(name="CountryISO3", role=ColumnRole.METADATA),
    ]
)

EXCHANGE_SYMBOLS_OUTPUT = OutputConfig(
    columns=[
        Column(name="Code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Exchange", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="Type", role=ColumnRole.METADATA),
    ]
)

CALENDAR_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="date", dtype="date"),
        Column(name="report_date", dtype="date"),
        Column(name="before_after_market", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="actual", dtype="numeric"),
        Column(name="estimate", dtype="numeric"),
        Column(name="difference", dtype="numeric"),
        Column(name="percent", dtype="numeric"),
    ]
)

NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="datetime"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="content"),
        Column(name="link", role=ColumnRole.METADATA),
        Column(name="symbols", role=ColumnRole.METADATA),
        Column(name="tags", role=ColumnRole.METADATA),
    ]
)

MACRO_OUTPUT = OutputConfig(
    columns=[
        Column(name="Date", role=ColumnRole.KEY, dtype="date"),
        Column(name="Value", dtype="numeric"),
        Column(name="Period", role=ColumnRole.METADATA),
        Column(name="LastUpdated", role=ColumnRole.METADATA),
    ]
)

TECHNICAL_OUTPUT = OutputConfig(
    columns=[
        Column(name="date", role=ColumnRole.KEY, dtype="date"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="*"),  # indicator-specific columns vary by function
    ]
)

INSIDER_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="date", dtype="date"),
        Column(name="ownerName", role=ColumnRole.METADATA),
        Column(name="ownerCik", role=ColumnRole.METADATA),
        Column(name="transactionType", role=ColumnRole.METADATA),
        Column(name="transactionDate", dtype="date"),
        Column(name="value", dtype="numeric"),
        Column(name="sharesOwned", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="*"),
    ]
)

SCREENER_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="sector", role=ColumnRole.METADATA),
        Column(name="industry", role=ColumnRole.METADATA),
        Column(name="market_capitalization", dtype="numeric"),
        Column(name="*"),
    ]
)
