"""Pydantic parameter models for the EODHD connectors.

Every ``@connector`` function in ``__init__.py`` accepts one of the classes
defined here as its typed ``params`` argument. These classes form part of
the public import surface — tests and downstream callers depend on them.
"""

from __future__ import annotations

from typing import Annotated, Literal

from parsimony.connector import Namespace
from pydantic import BaseModel, ConfigDict, Field

# Technical-indicator function names accepted by eodhd_technical. Declared
# as a module-level ``Literal`` alias so the ``EodhdTechnicalParams`` model
# stays concise and future additions happen in one place.
_EodhdTechnicalFunction = Literal[
    "sma",
    "ema",
    "wma",
    "volatility",
    "stochastic",
    "rsi",
    "stddev",
    "stochrsi",
    "slope",
    "dmi",
    "adx",
    "macd",
    "atr",
    "cci",
    "sar",
    "bbands",
    "splitadjusted",
    "avgvol",
    "avgvolacave",
    "williams_r",
]


class EodhdEodParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US or BARC.LSE",
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-15. Use as from_date='2024-01-15'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'"
    )
    period: Literal["d", "w", "m"] | None = Field(
        default=None,
        description="Aggregation period: d (daily), w (weekly), m (monthly). Default: d",
    )


class EodhdLiveParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US",
    )


class EodhdIntradayParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US",
    )
    interval: Literal["1m", "5m", "1h"] = Field(..., description="Intraday interval: 1m, 5m, or 1h")
    from_unix: int | None = Field(default=None, description="Start time as Unix timestamp (seconds since epoch)")
    to_unix: int | None = Field(default=None, description="End time as Unix timestamp (seconds since epoch)")


class EodhdBulkEodParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exchange: str = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Exchange code, e.g. US, LSE, XETRA, TSX",
    )
    date: str | None = Field(
        default=None, description="Trading date, ISO 8601, e.g. 2024-01-15. Defaults to last trading day."
    )


class EodhdDividendsParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ..., pattern=r"^[A-Z0-9._\-]+$", description="Ticker in EODHD format, e.g. AAPL.US"
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601. Use as from_date='2024-01-15'"
    )
    to_date: str | None = Field(default=None, alias="to", description="End date ISO 8601. Use as to_date='2024-12-31'")


class EodhdSplitsParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ..., pattern=r"^[A-Z0-9._\-]+$", description="Ticker in EODHD format, e.g. AAPL.US"
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601. Use as from_date='2024-01-15'"
    )
    to_date: str | None = Field(default=None, alias="to", description="End date ISO 8601. Use as to_date='2024-12-31'")


class EodhdSearchParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(..., description="Company name or partial ticker to search for, e.g. 'Apple' or 'AAPL'")
    limit: int = Field(default=50, description="Maximum number of results (default 50)")
    type: Literal["Q", "ETF", "FUND", "BOND", "INDEX"] | None = Field(
        default=None, description="Instrument type filter: Q (equity), ETF, FUND, BOND, INDEX"
    )


class EodhdExchangesParams(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EodhdExchangeSymbolsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exchange: str = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Exchange code, e.g. US, LSE, XETRA. Use eodhd_exchanges to list valid codes.",
    )
    type: Literal["common_stock", "preferred_stock", "stock", "etf", "fund"] | None = Field(
        default=None, description="Instrument type filter: common_stock, preferred_stock, stock, etf, fund"
    )


class EodhdFundamentalsParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ...,
        pattern=r"^[A-Z0-9._\-]+$",
        description="Ticker in EODHD format, e.g. AAPL.US or SPY.US (ETFs supported)",
    )


class EodhdCalendarParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["earnings", "ipo", "trends"] = Field(
        ..., description="Calendar type: earnings (EPS calendar), ipo (IPO calendar), trends (analyst trends)"
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-01. Use as from_date='2024-01-01'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-03-31. Use as to_date='2024-03-31'"
    )
    symbols: str | None = Field(
        default=None, description="Comma-separated EODHD ticker codes to filter, e.g. AAPL.US,MSFT.US"
    )


class EodhdNewsParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: str | None = Field(
        default=None,
        description="EODHD ticker to filter news, e.g. AAPL.US. Omit for market-wide news.",
    )
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-15. Use as from_date='2024-01-15'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'"
    )
    limit: int = Field(default=50, description="Max number of articles to return (default 50)")
    offset: int = Field(default=0, description="Offset for pagination (0-indexed)")


class EodhdMacroParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str = Field(
        ...,
        pattern=r"^[A-Z]{3}$",
        description="ISO 3-letter country code, e.g. USA, DEU, GBR, FRA, JPN",
    )
    indicator: str = Field(
        ...,
        description=(
            "Macro indicator code, e.g. gdp_current_usd, unemployment_total_percent, "
            "inflation_consumer_prices_annual, real_interest_rate, population_total"
        ),
    )


class EodhdMacroBulkParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    country: str = Field(
        ...,
        pattern=r"^[A-Z]{3}$",
        description="ISO 3-letter country code, e.g. USA, DEU, GBR",
    )
    topic: str | None = Field(
        default=None,
        description=(
            "Optional topic filter to narrow the result set. "
            "Verify valid values against EODHD macro-indicator documentation."
        ),
    )


class EodhdTechnicalParams(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ticker: Annotated[str, Namespace("eodhd_symbols")] = Field(
        ..., pattern=r"^[A-Z0-9._\-]+$", description="Ticker in EODHD format, e.g. AAPL.US"
    )
    function: _EodhdTechnicalFunction = Field(
        ...,
        description=(
            "Technical indicator function: sma, ema, rsi, macd, bbands, atr, stochastic, "
            "adx, cci, sar, williams_r, wma, volatility, stddev, dmi, slope, stochrsi, avgvol — "
            "see EODHD docs for full parameter set per function"
        ),
    )
    period: int = Field(default=50, description="Lookback period (number of bars, default 50)")
    from_date: str | None = Field(
        default=None, alias="from", description="Start date ISO 8601 e.g. 2024-01-01. Use as from_date='2024-01-01'"
    )
    to_date: str | None = Field(
        default=None, alias="to", description="End date ISO 8601 e.g. 2024-12-31. Use as to_date='2024-12-31'"
    )
    order: Literal["a", "d"] = Field(default="d", description="Sort order: a (ascending) or d (descending, default)")


class EodhdInsiderParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: Annotated[str, Namespace("eodhd_symbols")] | None = Field(
        default=None,
        description="EODHD ticker to filter by, e.g. AAPL.US. Omit for all recent transactions.",
    )
    limit: int = Field(default=100, description="Max transactions to return (default 100)")
    offset: int = Field(default=0, description="Offset for pagination (0-indexed)")


class EodhdScreenerParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filters: list[tuple[str, str, str]] | None = Field(
        default=None,
        description=(
            "List of filter triples [field, operator, value], e.g. "
            "[['market_capitalization', '>', '1000000000'], ['exchange', '=', 'US']]. "
            "Valid operators: >, <, =, >=, <=. "
            "Common fields: market_capitalization, earnings_share, dividend_yield, "
            "pe_ratio, revenue, sector, exchange."
        ),
    )
    signals: str | None = Field(
        default=None,
        description="Signal filter, e.g. 'bookvalue_neg,wallstreet_lo'. See EODHD screener docs.",
    )
    sort: str | None = Field(
        default=None,
        description="Field to sort by, e.g. market_capitalization",
    )
    order: Literal["asc", "desc"] = Field(default="desc", description="Sort order: asc or desc (default)")
    limit: int = Field(default=50, description="Max results (default 50)")
    offset: int = Field(default=0, description="Offset for pagination (0-indexed)")
