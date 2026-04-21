"""Pydantic parameter models for the Alpha Vantage connectors.

Every ``@connector`` / ``@enumerator`` function in ``__init__.py`` accepts
one of the classes defined here as its typed ``params`` argument. These
classes form part of the public import surface — tests and downstream
callers depend on them.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field

# Economic-indicator function names accepted by alpha_vantage_econ. Declared
# as a tuple at module scope so the Pydantic ``Literal[...]`` type narrowing
# stays AST-visible and picks up future additions without code duplication.
_ECON_FUNCTIONS = (
    "REAL_GDP",
    "REAL_GDP_PER_CAPITA",
    "TREASURY_YIELD",
    "FEDERAL_FUNDS_RATE",
    "CPI",
    "INFLATION",
    "RETAIL_SALES",
    "DURABLES",
    "UNEMPLOYMENT",
    "NONFARM_PAYROLL",
)

# Technical-indicator function names accepted by alpha_vantage_technical.
_TECHNICAL_INDICATORS = (
    "SMA",
    "EMA",
    "WMA",
    "DEMA",
    "TEMA",
    "TRIMA",
    "KAMA",
    "MAMA",
    "VWAP",
    "T3",
    "RSI",
    "WILLR",
    "ADX",
    "ADXR",
    "APO",
    "PPO",
    "MOM",
    "BOP",
    "CCI",
    "CMO",
    "ROC",
    "ROCR",
    "AROON",
    "AROONOSC",
    "MFI",
    "TRIX",
    "ULTOSC",
    "DX",
    "MINUS_DI",
    "PLUS_DI",
    "MINUS_DM",
    "PLUS_DM",
    "BBANDS",
    "MIDPOINT",
    "MIDPRICE",
    "SAR",
    "TRANGE",
    "ATR",
    "NATR",
    "AD",
    "ADOSC",
    "OBV",
    "HT_TRENDLINE",
    "HT_SINE",
    "HT_TRENDMODE",
    "HT_DCPERIOD",
    "HT_DCPHASE",
    "HT_PHASOR",
    "STOCH",
    "STOCHF",
    "STOCHRSI",
    "MACD",
    "MACDEXT",
)


class AlphaVantageSearchParams(BaseModel):
    """Search Alpha Vantage for stocks, ETFs, and mutual funds by name or ticker."""

    keywords: str = Field(..., min_length=1, description="Search term, e.g. 'apple' or 'AAPL'")


class AlphaVantageQuoteParams(BaseModel):
    """Real-time quote for a single stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


class AlphaVantageDailyParams(BaseModel):
    """Daily OHLCV time series for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    outputsize: Literal["compact", "full"] = Field(
        default="compact",
        description="'compact' returns last 100 data points; 'full' returns 20+ years of history.",
    )


class AlphaVantageOverviewParams(BaseModel):
    """Company overview / fundamentals for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


class AlphaVantageStatementParams(BaseModel):
    """Financial statement for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    period: Literal["annual", "quarterly"] = Field(
        default="annual",
        description="'annual' for yearly reports; 'quarterly' for quarterly.",
    )


class AlphaVantageEarningsParams(BaseModel):
    """Earnings data for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


class AlphaVantageFxRateParams(BaseModel):
    """Real-time exchange rate between two currencies (forex or crypto)."""

    from_currency: str = Field(..., description="Source currency code, e.g. 'EUR', 'BTC'")
    to_currency: str = Field(..., description="Target currency code, e.g. 'USD', 'JPY'")


class AlphaVantageFxDailyParams(BaseModel):
    """Daily forex time series."""

    from_symbol: str = Field(..., description="Source currency code, e.g. 'EUR'")
    to_symbol: str = Field(..., description="Target currency code, e.g. 'USD'")
    outputsize: Literal["compact", "full"] = Field(
        default="compact",
        description="'compact' returns last 100 data points; 'full' returns full history.",
    )


class AlphaVantageCryptoDailyParams(BaseModel):
    """Daily crypto time series."""

    symbol: str = Field(..., description="Crypto symbol, e.g. 'BTC', 'ETH'")
    market: str = Field(default="USD", description="Exchange market currency, e.g. 'USD', 'EUR'")


class AlphaVantageEconParams(BaseModel):
    """US economic indicator time series."""

    function: Literal[_ECON_FUNCTIONS] = Field(  # type: ignore[valid-type]
        ...,
        description=(
            "Indicator: REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD, "
            "FEDERAL_FUNDS_RATE, CPI, INFLATION, RETAIL_SALES, DURABLES, "
            "UNEMPLOYMENT, NONFARM_PAYROLL."
        ),
    )
    interval: Literal["daily", "weekly", "monthly", "quarterly", "annual"] | None = Field(
        default=None,
        description=(
            "Data frequency. GDP: quarterly/annual. Rates: daily/weekly/monthly. "
            "Most indicators: monthly or annual only. Invalid values silently ignored."
        ),
    )
    maturity: Literal["3month", "2year", "5year", "7year", "10year", "30year"] | None = Field(
        default=None,
        description="Treasury maturity (TREASURY_YIELD only). Default: 10year.",
    )


class AlphaVantageNewsParams(BaseModel):
    """News sentiment articles."""

    tickers: str | None = Field(
        default=None,
        description="Comma-separated ticker(s), e.g. 'AAPL' or 'AAPL,MSFT'. Omit for general news.",
    )
    topics: str | None = Field(
        default=None,
        description=(
            "Comma-separated topics: technology, earnings, ipo, mergers_and_acquisitions, "
            "financial_markets, economy_fiscal, economy_monetary, economy_macro, "
            "energy_transportation, finance, life_sciences, manufacturing, real_estate, "
            "retail_wholesale, blockchain."
        ),
    )
    sort: Literal["LATEST", "EARLIEST", "RELEVANCE"] = Field(default="LATEST", description="Sort order for results.")
    limit: int = Field(default=50, ge=1, le=1000, description="Number of results (max 1000).")


class AlphaVantageTopMoversParams(BaseModel):
    """Top gainers, losers, and most actively traded."""

    pass


class AlphaVantageOptionsParams(BaseModel):
    """Historical options chain for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    date: str | None = Field(
        default=None,
        description="Options date (YYYY-MM-DD). Omit for latest available.",
    )


class AlphaVantageWeeklyParams(BaseModel):
    """Weekly OHLCV time series for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


class AlphaVantageMonthlyParams(BaseModel):
    """Monthly OHLCV time series for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )


class AlphaVantageIntradayParams(BaseModel):
    """Intraday OHLCV time series for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    interval: Literal["1min", "5min", "15min", "30min", "60min"] = Field(
        default="60min",
        description="Time interval: 1min, 5min, 15min, 30min, or 60min.",
    )
    outputsize: Literal["compact", "full"] = Field(
        default="compact",
        description="'compact' returns last 100 data points; 'full' returns full intraday history.",
    )


class AlphaVantageFxWeeklyParams(BaseModel):
    """Weekly forex time series."""

    from_symbol: str = Field(..., description="Source currency code, e.g. 'EUR'")
    to_symbol: str = Field(..., description="Target currency code, e.g. 'USD'")


class AlphaVantageFxMonthlyParams(BaseModel):
    """Monthly forex time series."""

    from_symbol: str = Field(..., description="Source currency code, e.g. 'EUR'")
    to_symbol: str = Field(..., description="Target currency code, e.g. 'USD'")


class AlphaVantageCryptoWeeklyParams(BaseModel):
    """Weekly crypto time series."""

    symbol: str = Field(..., description="Crypto symbol, e.g. 'BTC', 'ETH'")
    market: str = Field(default="USD", description="Exchange market currency, e.g. 'USD', 'EUR'")


class AlphaVantageCryptoMonthlyParams(BaseModel):
    """Monthly crypto time series."""

    symbol: str = Field(..., description="Crypto symbol, e.g. 'BTC', 'ETH'")
    market: str = Field(default="USD", description="Exchange market currency, e.g. 'USD', 'EUR'")


class AlphaVantageEtfProfileParams(BaseModel):
    """ETF profile including holdings and sector allocation."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="ETF ticker, e.g. 'SPY', 'QQQ'. Use alpha_vantage_search to find ETFs.",
    )


class AlphaVantageEarningsCalendarParams(BaseModel):
    """Upcoming earnings release dates."""

    horizon: Literal["3month", "6month", "12month"] = Field(
        default="3month",
        description="Lookahead window: 3month, 6month, or 12month.",
    )
    symbol: str | None = Field(
        default=None,
        description="Optional ticker to filter by, e.g. 'IBM'. Omit for all companies.",
    )


class AlphaVantageIpoCalendarParams(BaseModel):
    """Upcoming and recent IPOs."""

    pass


class AlphaVantageTechnicalParams(BaseModel):
    """Technical indicator for a stock symbol."""

    symbol: Annotated[str, "ns:alpha_vantage"] = Field(
        ...,
        description="Stock ticker, e.g. 'IBM'. Use alpha_vantage_search to resolve symbols.",
    )
    function: Literal[_TECHNICAL_INDICATORS] = Field(  # type: ignore[valid-type]
        ...,
        description=(
            "Indicator function. Common: SMA, EMA, RSI, MACD, BBANDS, STOCH, ADX, "
            "CCI, WILLR, MFI, OBV, ATR, VWAP, AROON, SAR, TRIX, APO, PPO."
        ),
    )
    interval: Literal["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"] = Field(
        default="daily",
        description="Time interval for the indicator.",
    )
    time_period: int = Field(
        default=20,
        ge=1,
        description="Number of data points for the calculation (e.g. 20 for SMA-20).",
    )
    series_type: Literal["close", "open", "high", "low"] = Field(
        default="close",
        description="Price type to use for the calculation.",
    )


class AlphaVantageMetalSpotParams(BaseModel):
    """Real-time spot price for gold or silver."""

    symbol: Literal["GOLD", "XAU", "SILVER", "XAG"] = Field(
        ...,
        description="Metal symbol: GOLD or XAU (gold), SILVER or XAG (silver).",
    )


class AlphaVantageMetalHistoryParams(BaseModel):
    """Historical prices for gold or silver."""

    symbol: Literal["GOLD", "XAU", "SILVER", "XAG"] = Field(
        ...,
        description="Metal symbol: GOLD or XAU (gold), SILVER or XAG (silver).",
    )
    interval: Literal["daily", "weekly", "monthly"] = Field(
        default="monthly",
        description="Data frequency: daily, weekly, or monthly.",
    )


class AlphaVantageListingParams(BaseModel):
    """Parameters for enumerating listed securities."""

    state: Literal["active", "delisted"] = Field(
        default="active",
        description="'active' for current listings; 'delisted' for historical.",
    )


__all__ = [
    "AlphaVantageCryptoDailyParams",
    "AlphaVantageCryptoMonthlyParams",
    "AlphaVantageCryptoWeeklyParams",
    "AlphaVantageDailyParams",
    "AlphaVantageEarningsCalendarParams",
    "AlphaVantageEarningsParams",
    "AlphaVantageEconParams",
    "AlphaVantageEtfProfileParams",
    "AlphaVantageFxDailyParams",
    "AlphaVantageFxMonthlyParams",
    "AlphaVantageFxRateParams",
    "AlphaVantageFxWeeklyParams",
    "AlphaVantageIntradayParams",
    "AlphaVantageIpoCalendarParams",
    "AlphaVantageListingParams",
    "AlphaVantageMetalHistoryParams",
    "AlphaVantageMetalSpotParams",
    "AlphaVantageMonthlyParams",
    "AlphaVantageNewsParams",
    "AlphaVantageOptionsParams",
    "AlphaVantageOverviewParams",
    "AlphaVantageQuoteParams",
    "AlphaVantageSearchParams",
    "AlphaVantageStatementParams",
    "AlphaVantageTechnicalParams",
    "AlphaVantageTopMoversParams",
    "AlphaVantageWeeklyParams",
]
