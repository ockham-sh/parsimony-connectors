"""Pydantic parameter models for the FMP connectors.

Grouped by the kernel contract: every ``@connector`` function in
``__init__.py`` accepts one of the classes defined here as its typed
``params`` argument. These classes form part of the public import surface
— tests and downstream callers depend on them.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class FmpSymbolParams(BaseModel):
    """Single-symbol requests."""

    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock ticker symbol (e.g. AAPL)"
    )


class FmpSymbolsParams(BaseModel):
    """Comma-separated symbols for batch endpoints."""

    symbols: str = Field(..., description="Comma-separated stock symbols (e.g. AAPL,MSFT,GOOGL)")


class FmpSearchParams(BaseModel):
    query: str = Field(..., description="Company name fragment or partial ticker (e.g. 'Deutsche Bank' or 'DBK')")
    limit: int = Field(default=20, description="Maximum number of results (default 20)")
    exchange: str | None = Field(default=None, description="Restrict to exchange (e.g. NYSE, NASDAQ, XETRA)")


class FmpFinancialStatementParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock ticker symbol (e.g. AAPL)"
    )
    period: str = Field(default="annual", description="Reporting period (annual or quarter)")
    limit: int = Field(default=5, description="Maximum number of periods to return")


class FmpHistoricalPricesParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock ticker symbol (e.g. AAPL)"
    )
    frequency: str = Field(
        default="daily",
        description="Price frequency: daily, dividend_adjusted, 1min, 5min, 15min, 30min, 1hour, 4hour",
    )
    from_date: str | None = Field(default=None, alias="from", description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, alias="to", description="End date (YYYY-MM-DD)")

    model_config = {"populate_by_name": True}


class FmpTaxonomyParams(BaseModel):
    type: Literal["sectors", "industries", "exchanges", "symbols_with_financials"] = Field(
        ..., description="Taxonomy type: sectors, industries, exchanges, or symbols_with_financials"
    )


class FmpNewsParams(BaseModel):
    type: Literal["news", "press_releases"] = Field(
        ..., description="news for third-party articles, press_releases for official company IR"
    )
    symbols: str = Field(..., description="Comma-separated stock symbols (e.g. AAPL,MSFT)")
    from_date: str | None = Field(default=None, alias="from", description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, alias="to", description="End date (YYYY-MM-DD)")
    limit: int = Field(default=20, description="Max records (default 20, max 250)")
    page: int = Field(default=0, description="Page offset (0-indexed)")

    model_config = {"populate_by_name": True}


class FmpInsiderTradesParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    limit: int = Field(default=20, description="Max trades to return (default 20)")
    page: int = Field(default=0, description="Page offset (0-indexed)")


class FmpInstitutionalPositionsParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    year: str = Field(..., description="Reporting year (e.g. 2024)")
    quarter: str = Field(..., description="Reporting quarter (1, 2, 3, or 4)")


class FmpEarningsTranscriptParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    year: str = Field(..., description="Fiscal year (e.g. 2024)")
    quarter: str = Field(..., description="Fiscal quarter (1, 2, 3, or 4)")


class FmpCorporateHistoryParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    event_type: Literal["earnings", "dividends", "splits"] = Field(
        ..., description="Type of corporate event"
    )
    limit: int = Field(default=10, description="Max historical records (default 10)")


class FmpEventCalendarParams(BaseModel):
    event_type: Literal["earnings", "dividends", "splits"] = Field(
        ..., description="Calendar type: earnings, dividends, or splits"
    )
    from_date: str | None = Field(default=None, alias="from", description="Start date (YYYY-MM-DD, max 90-day range)")
    to_date: str | None = Field(default=None, alias="to", description="End date (YYYY-MM-DD)")

    model_config = {"populate_by_name": True}


class FmpAnalystEstimatesParams(BaseModel):
    symbol: Annotated[str, "ns:fmp_symbols"] = Field(
        ..., description="Stock symbol (e.g. AAPL)"
    )
    period: str = Field(default="annual", description="annual or quarter")
    limit: int = Field(default=4, description="Number of estimate periods (default 4)")


class FmpIndexConstituentsParams(BaseModel):
    index: Literal["SP500", "NASDAQ", "DOW_JONES"] = Field(
        ..., description="Index: SP500, NASDAQ, or DOW_JONES"
    )


class FmpMarketMoversParams(BaseModel):
    type: Literal["gainers", "losers", "most_actives"] = Field(
        ..., description="gainers, losers, or most_actives"
    )


class FmpScreenerParams(BaseModel):
    """Global equity screener with pushdown filters and TTM-enrichment post-filtering."""

    # Pushdown filters (applied at FMP screener API level)
    sector: str | None = Field(default=None, description="Filter by sector (e.g. 'Technology')")
    industry: str | None = Field(default=None, description="Filter by industry (e.g. 'Consumer Electronics')")
    country: str | None = Field(
        default=None,
        description="Country code (e.g. 'US', 'DE'). Single value; for multiple use where_clause.",
    )
    exchange: str | None = Field(default=None, description="Exchange code (e.g. 'NASDAQ', 'NYSE'). Single value.")
    market_cap_min: float | None = Field(default=None, description="Minimum market cap")
    market_cap_max: float | None = Field(default=None, description="Maximum market cap")
    price_min: float | None = Field(default=None, description="Minimum stock price")
    price_max: float | None = Field(default=None, description="Maximum stock price")
    volume_min: float | None = Field(default=None, description="Minimum trading volume")
    volume_max: float | None = Field(default=None, description="Maximum trading volume")
    beta_min: float | None = Field(default=None, description="Minimum beta")
    beta_max: float | None = Field(default=None, description="Maximum beta")
    dividend_min: float | None = Field(default=None, description="Minimum last annual dividend")
    dividend_max: float | None = Field(default=None, description="Maximum last annual dividend")
    is_etf: bool | None = Field(default=None, description="Include (True) or exclude (False) ETFs")
    is_fund: bool | None = Field(default=None, description="Include (True) or exclude (False) funds")
    is_actively_trading: bool | None = Field(default=None, description="Restrict to actively trading (True)")

    # Enrichment / residual filtering
    where_clause: str | None = Field(
        default=None,
        description=(
            "pandas df.query() filter applied after enrichment."
            " Can reference screener, key-metrics-ttm, or ratios-ttm columns."
        ),
    )
    sort_by: str | None = Field(
        default=None, description="Column to sort by (e.g. 'marketCap', 'freeCashFlowYieldTTM')"
    )
    sort_order: str = Field(default="desc", description="Sort direction: 'asc' or 'desc'")
    limit: int = Field(default=100, description="Max rows to return (default 100)")
    prefilter_limit: int | None = Field(
        default=None,
        description=(
            "Max symbols from screener before enrichment. Default max(limit, 500)."
            " Increase to 1000-2000 for broad global searches sorted by TTM columns."
        ),
    )
    fields: list[str] | None = Field(
        default=None,
        description=(
            "Output columns to include. symbol always returned."
            " Omit for all columns. When specified, skips unnecessary enrichment calls."
        ),
    )


__all__ = [
    "FmpAnalystEstimatesParams",
    "FmpCorporateHistoryParams",
    "FmpEarningsTranscriptParams",
    "FmpEventCalendarParams",
    "FmpFinancialStatementParams",
    "FmpHistoricalPricesParams",
    "FmpIndexConstituentsParams",
    "FmpInsiderTradesParams",
    "FmpInstitutionalPositionsParams",
    "FmpMarketMoversParams",
    "FmpNewsParams",
    "FmpScreenerParams",
    "FmpSearchParams",
    "FmpSymbolParams",
    "FmpSymbolsParams",
    "FmpTaxonomyParams",
]
