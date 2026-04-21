"""Pydantic parameter models for the Tiingo connectors.

Every ``@connector`` / ``@enumerator`` function in ``__init__.py`` accepts
one of the classes defined here as its typed ``params`` argument. These
classes form part of the public import surface — tests and downstream
callers depend on them.
"""

from __future__ import annotations

import re
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Regex guard for values that are interpolated directly into request paths
# (``/tiingo/daily/<ticker>/prices`` etc.). Anything outside the allowed
# character set is rejected at validation time before the URL is built.
_TICKER_RE = re.compile(r"^[a-zA-Z0-9._\-]+$")


class TiingoSearchParams(BaseModel):
    """Search Tiingo for stocks, ETFs, mutual funds, and crypto by name or ticker."""

    query: str = Field(..., min_length=1, description="Search term, e.g. 'apple' or 'AAPL'")
    limit: int = Field(default=25, ge=1, le=100, description="Max results to return (1-100)")


class TiingoEodParams(BaseModel):
    """Historical end-of-day prices for a stock."""

    ticker: Annotated[str, "ns:tiingo_ticker"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use tiingo_search to resolve tickers."
    )
    start_date: str | None = Field(default=None, description="Start date ISO 8601, e.g. '2024-01-01'.")
    end_date: str | None = Field(default=None, description="End date ISO 8601, e.g. '2024-12-31'.")

    @field_validator("ticker")
    @classmethod
    def _path_safe_ticker(cls, v: str) -> str:
        if not _TICKER_RE.match(v):
            raise ValueError(f"ticker contains unsafe characters for URL path: {v!r}")
        return v


class TiingoIexParams(BaseModel):
    """Real-time IEX quote for one or more stock tickers."""

    tickers: str = Field(
        ...,
        description="Comma-separated tickers, e.g. 'AAPL' or 'AAPL,MSFT,TSLA'. Use tiingo_search to resolve.",
    )


class TiingoIexHistParams(BaseModel):
    """Historical IEX intraday prices for a stock."""

    ticker: Annotated[str, "ns:tiingo_ticker"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use tiingo_search to resolve tickers."
    )
    start_date: str | None = Field(default=None, description="Start date ISO 8601, e.g. '2024-01-01'.")
    end_date: str | None = Field(default=None, description="End date ISO 8601, e.g. '2024-01-05'.")
    resample_freq: str = Field(
        default="1hour",
        description="Resample frequency: '1min', '5min', '15min', '30min', '1hour', '2hour', '4hour'.",
    )

    @field_validator("ticker")
    @classmethod
    def _path_safe_ticker(cls, v: str) -> str:
        if not _TICKER_RE.match(v):
            raise ValueError(f"ticker contains unsafe characters for URL path: {v!r}")
        return v


class TiingoMetaParams(BaseModel):
    """Company metadata for a stock ticker."""

    ticker: Annotated[str, "ns:tiingo_ticker"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use tiingo_search to resolve tickers."
    )

    @field_validator("ticker")
    @classmethod
    def _path_safe_ticker(cls, v: str) -> str:
        if not _TICKER_RE.match(v):
            raise ValueError(f"ticker contains unsafe characters for URL path: {v!r}")
        return v


class TiingoFundamentalsMetaParams(BaseModel):
    """Fundamentals metadata: sector, industry, SIC codes, and more."""

    tickers: str = Field(
        ...,
        description="Comma-separated tickers, e.g. 'AAPL' or 'AAPL,MSFT'. Use tiingo_search to resolve.",
    )


class TiingoDefinitionsParams(BaseModel):
    """List all fundamental metric definitions (codes, names, units)."""

    pass


class TiingoNewsParams(BaseModel):
    """News articles from Tiingo (requires Power+ plan)."""

    model_config = ConfigDict(populate_by_name=True)

    tickers: str | None = Field(
        default=None,
        description="Comma-separated tickers to filter, e.g. 'AAPL,MSFT'. Omit for all.",
    )
    source: str | None = Field(default=None, description="News source filter, e.g. 'bloomberg.com'.")
    start_date: str | None = Field(default=None, description="Start date ISO 8601, e.g. '2024-01-01'.")
    end_date: str | None = Field(default=None, description="End date ISO 8601, e.g. '2024-12-31'.")
    limit: int = Field(default=50, ge=1, le=100, description="Max articles (1-100)")


class TiingoCryptoPricesParams(BaseModel):
    """Historical crypto prices."""

    tickers: str = Field(..., description="Crypto pair, e.g. 'btcusd' or 'ethusd'. Use lowercase.")
    start_date: str | None = Field(default=None, description="Start date ISO 8601, e.g. '2024-01-01'.")
    end_date: str | None = Field(default=None, description="End date ISO 8601, e.g. '2024-12-31'.")
    resample_freq: str = Field(
        default="1day",
        description="Resample frequency: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day'.",
    )


class TiingoCryptoTopParams(BaseModel):
    """Real-time crypto top-of-book quotes."""

    tickers: str = Field(..., description="Comma-separated crypto pairs, e.g. 'btcusd' or 'btcusd,ethusd'. Lowercase.")


class TiingoFxPricesParams(BaseModel):
    """Historical forex prices."""

    tickers: str = Field(..., description="Forex pair, e.g. 'eurusd' or 'gbpjpy'. Use lowercase.")
    start_date: str | None = Field(default=None, description="Start date ISO 8601, e.g. '2024-01-01'.")
    end_date: str | None = Field(default=None, description="End date ISO 8601, e.g. '2024-12-31'.")
    resample_freq: str = Field(
        default="1day",
        description="Resample frequency: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day'.",
    )


class TiingoFxTopParams(BaseModel):
    """Real-time forex top-of-book quotes."""

    tickers: str = Field(..., description="Comma-separated forex pairs, e.g. 'eurusd' or 'eurusd,gbpjpy'. Lowercase.")


class TiingoEnumerateParams(BaseModel):
    """Parameters for enumerating Tiingo supported tickers."""

    pass


__all__ = [
    "TiingoCryptoPricesParams",
    "TiingoCryptoTopParams",
    "TiingoDefinitionsParams",
    "TiingoEnumerateParams",
    "TiingoEodParams",
    "TiingoFundamentalsMetaParams",
    "TiingoFxPricesParams",
    "TiingoFxTopParams",
    "TiingoIexHistParams",
    "TiingoIexParams",
    "TiingoMetaParams",
    "TiingoNewsParams",
    "TiingoSearchParams",
]
