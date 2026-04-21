"""Pydantic parameter models for the Finnhub connectors.

Every ``@connector`` / ``@enumerator`` function in ``__init__.py`` accepts
one of the classes defined here as its typed ``params`` argument. These
classes form part of the public import surface — tests and downstream
callers depend on them.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class FinnhubSearchParams(BaseModel):
    """Search Finnhub for stocks, ETFs, and indices by name or ticker."""

    query: str = Field(..., min_length=1, description="Search term, e.g. 'apple' or 'AAPL'")


class FinnhubQuoteParams(BaseModel):
    """Real-time quote for a single stock symbol."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )


class FinnhubProfileParams(BaseModel):
    """Company profile for a single stock symbol."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )


class FinnhubPeersParams(BaseModel):
    """Peer companies for a given stock symbol."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )


class FinnhubRecommendationParams(BaseModel):
    """Analyst buy/sell/hold recommendations for a stock."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )


class FinnhubEarningsParams(BaseModel):
    """Historical EPS actuals and estimates for a stock."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )


class FinnhubBasicFinancialsParams(BaseModel):
    """Fundamental financial metrics for a stock."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )


class FinnhubCompanyNewsParams(BaseModel):
    """News articles for a specific company."""

    symbol: Annotated[str, "ns:finnhub_symbol"] = Field(
        ..., description="Stock ticker, e.g. 'AAPL'. Use finnhub_search to resolve symbols."
    )
    from_date: str = Field(
        ...,
        description="Start date ISO 8601, e.g. '2024-01-01'. Free tier: recent months only.",
    )
    to_date: str = Field(
        ...,
        description="End date ISO 8601, e.g. '2024-01-31'.",
    )


class FinnhubMarketNewsParams(BaseModel):
    """Market-wide news by category."""

    category: Literal["general", "forex", "crypto", "merger"] = Field(
        default="general",
        description="News category: 'general', 'forex', 'crypto', or 'merger'",
    )


class FinnhubEarningsCalendarParams(BaseModel):
    """Earnings release calendar between two dates."""

    from_date: str = Field(
        ...,
        description="Start date ISO 8601, e.g. '2024-01-01'. Free tier: recent/upcoming dates.",
    )
    to_date: str = Field(..., description="End date ISO 8601, e.g. '2024-01-31'.")
    symbol: str | None = Field(
        default=None,
        description="Optional ticker to filter by, e.g. 'AAPL'. Omit for all companies.",
    )


class FinnhubIpoCalendarParams(BaseModel):
    """IPO calendar between two dates."""

    from_date: str = Field(
        ...,
        description="Start date ISO 8601, e.g. '2024-01-01'.",
    )
    to_date: str = Field(..., description="End date ISO 8601, e.g. '2024-03-31'.")


class FinnhubEnumerateParams(BaseModel):
    """Parameters for enumerating Finnhub symbols."""

    exchange: str = Field(
        default="US",
        description="Exchange code, e.g. 'US' for all US-listed equities (~30 000 symbols).",
    )


__all__ = [
    "FinnhubBasicFinancialsParams",
    "FinnhubCompanyNewsParams",
    "FinnhubEarningsCalendarParams",
    "FinnhubEarningsParams",
    "FinnhubEnumerateParams",
    "FinnhubIpoCalendarParams",
    "FinnhubMarketNewsParams",
    "FinnhubPeersParams",
    "FinnhubProfileParams",
    "FinnhubQuoteParams",
    "FinnhubRecommendationParams",
    "FinnhubSearchParams",
]
