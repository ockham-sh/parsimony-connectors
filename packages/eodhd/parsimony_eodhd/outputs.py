"""Declarative output schemas for the EODHD connectors.

One :class:`OutputSpec` per connector that projects a shaped DataFrame
out of EODHD's raw JSON. Columns declared here are the contract with the
MCP tool catalog — renaming or re-ordering them is a breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

EOD_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="adjusted_close"),
        Column(name="volume"),
    ]
)

LIVE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        # EODHD returns ``timestamp`` as a raw Unix epoch int; left un-coerced —
        # OutputSpec never transforms data (see parsimony.result.Column).
        Column(name="timestamp", role=ColumnRole.METADATA),
        Column(name="gmtoffset", role=ColumnRole.METADATA),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="volume"),
        Column(name="previousClose"),
        Column(name="change"),
        Column(name="change_p"),
    ]
)

INTRADAY_OUTPUT = OutputSpec(
    columns=[
        Column(name="timestamp", role=ColumnRole.KEY),
        Column(name="datetime", role=ColumnRole.METADATA),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="volume"),
    ]
)

BULK_EOD_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange_short_name", role=ColumnRole.METADATA),
        Column(name="date"),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="adjusted_close"),
        Column(name="volume"),
    ]
)

DIVIDENDS_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY),
        Column(name="declarationDate"),
        Column(name="recordDate"),
        Column(name="paymentDate"),
        Column(name="period", role=ColumnRole.METADATA),
        Column(name="value"),
        Column(name="unadjustedValue"),
        Column(name="currency", role=ColumnRole.METADATA),
    ]
)

SPLITS_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY),
        Column(name="split"),
    ]
)

SEARCH_OUTPUT = OutputSpec(
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

EXCHANGES_OUTPUT = OutputSpec(
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

EXCHANGE_SYMBOLS_OUTPUT = OutputSpec(
    columns=[
        Column(name="Code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="Name", role=ColumnRole.TITLE),
        Column(name="Country", role=ColumnRole.METADATA),
        Column(name="Exchange", role=ColumnRole.METADATA),
        Column(name="Currency", role=ColumnRole.METADATA),
        Column(name="Type", role=ColumnRole.METADATA),
        # Live payload key is ``Isin`` (not ``ISIN`` as on the search endpoint).
        Column(name="Isin", role=ColumnRole.METADATA),
    ]
)

CALENDAR_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="date"),
        Column(name="report_date"),
        Column(name="before_after_market", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="actual"),
        Column(name="estimate"),
        Column(name="difference"),
        Column(name="percent"),
    ]
)

NEWS_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="content"),
        Column(name="link", role=ColumnRole.METADATA),
        Column(name="symbols", role=ColumnRole.METADATA),
        Column(name="tags", role=ColumnRole.METADATA),
    ]
)

MACRO_OUTPUT = OutputSpec(
    columns=[
        # EODHD macro-indicator rows: CountryCode, CountryName, Indicator, Date,
        # Period, Value. (No LastUpdated — do not declare a column the payload
        # cannot populate.) CountryName/Indicator/CountryCode fold in as DATA.
        Column(name="Date", role=ColumnRole.KEY),
        Column(name="Value"),
        Column(name="Period", role=ColumnRole.METADATA),
    ]
)

TECHNICAL_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.KEY),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="volume"),
        Column(name="*"),  # indicator-specific columns vary by function
    ]
)

INSIDER_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="date"),
        Column(name="ownerName", role=ColumnRole.METADATA),
        Column(name="ownerCik", role=ColumnRole.METADATA),
        Column(name="transactionType", role=ColumnRole.METADATA),
        Column(name="transactionDate"),
        Column(name="value"),
        Column(name="sharesOwned"),
        Column(name="change"),
        Column(name="*"),
    ]
)

SCREENER_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="eodhd_symbols"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="exchange", role=ColumnRole.METADATA),
        Column(name="currency", role=ColumnRole.METADATA),
        Column(name="sector", role=ColumnRole.METADATA),
        Column(name="industry", role=ColumnRole.METADATA),
        Column(name="market_capitalization"),
        Column(name="*"),
    ]
)
