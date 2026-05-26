"""EODHD source: typed connectors per endpoint.

API docs: https://eodhd.com/financial-apis/api-for-historical-data-and-volumes/
Authentication: API token via ``?api_token=<key>`` query param.
Base URL: https://eodhd.com/api

Provides 17 connectors covering the full EODHD REST surface:
  - Market data: EOD prices, live quotes, intraday, bulk EOD
  - Corporate actions: dividends, splits
  - Reference: search, exchanges, exchange symbol lists
  - Fundamentals (raw dict — nested JSON blob)
  - Calendars: earnings, IPO, trends
  - News
  - Macro indicators
  - Technical indicators
  - Insider transactions
  - Screener

Internal layout (not part of the public contract):

* :mod:`parsimony_eodhd._http` — shared transport, unified error mapping,
  URL redaction, Retry-After parsing, JSON fetch helper.
* :mod:`parsimony_eodhd.params` — Pydantic parameter models.
* :mod:`parsimony_eodhd.outputs` — declarative :class:`OutputConfig` schemas.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from parsimony.connector import Connectors, connector

from parsimony_eodhd._http import eodhd_fetch as _eodhd_fetch
from parsimony_eodhd._http import make_http as _make_http
from parsimony_eodhd.outputs import BULK_EOD_OUTPUT as _BULK_EOD_OUTPUT
from parsimony_eodhd.outputs import CALENDAR_OUTPUT as _CALENDAR_OUTPUT
from parsimony_eodhd.outputs import DIVIDENDS_OUTPUT as _DIVIDENDS_OUTPUT
from parsimony_eodhd.outputs import EOD_OUTPUT as _EOD_OUTPUT
from parsimony_eodhd.outputs import EXCHANGE_SYMBOLS_OUTPUT as _EXCHANGE_SYMBOLS_OUTPUT
from parsimony_eodhd.outputs import EXCHANGES_OUTPUT as _EXCHANGES_OUTPUT
from parsimony_eodhd.outputs import INSIDER_OUTPUT as _INSIDER_OUTPUT
from parsimony_eodhd.outputs import INTRADAY_OUTPUT as _INTRADAY_OUTPUT
from parsimony_eodhd.outputs import LIVE_OUTPUT as _LIVE_OUTPUT
from parsimony_eodhd.outputs import MACRO_OUTPUT as _MACRO_OUTPUT
from parsimony_eodhd.outputs import NEWS_OUTPUT as _NEWS_OUTPUT
from parsimony_eodhd.outputs import SCREENER_OUTPUT as _SCREENER_OUTPUT
from parsimony_eodhd.outputs import SEARCH_OUTPUT as _SEARCH_OUTPUT
from parsimony_eodhd.outputs import SPLITS_OUTPUT as _SPLITS_OUTPUT
from parsimony_eodhd.outputs import TECHNICAL_OUTPUT as _TECHNICAL_OUTPUT
from parsimony_eodhd.params import (
    EodhdBulkEodParams,
    EodhdCalendarParams,
    EodhdDividendsParams,
    EodhdEodParams,
    EodhdExchangeSymbolsParams,
    EodhdFundamentalsParams,
    EodhdInsiderParams,
    EodhdIntradayParams,
    EodhdLiveParams,
    EodhdMacroBulkParams,
    EodhdMacroParams,
    EodhdNewsParams,
    EodhdScreenerParams,
    EodhdSearchParams,
    EodhdSplitsParams,
    EodhdTechnicalParams,
    _EodhdTechnicalFunction,
)

_LATENCY_TIMEOUT: float = 10.0
_BULK_TIMEOUT: float = 60.0


# ---------------------------------------------------------------------------
# Market Data — Connectors
# ---------------------------------------------------------------------------


@connector(output=_EOD_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_eod(
    ticker: Annotated[str, 'ns:eodhd_symbols'],
    from_date: str | None = None,
    to_date: str | None = None,
    period: Literal['d', 'w', 'm'] | None = None,
    *,
    api_key: str
) -> Any:
    """[Free+] Fetch end-of-day OHLCV prices for a ticker. Supports daily, weekly, and monthly
    aggregation. Use from/to to limit the date range (ISO 8601). Empty result may indicate an
    invalid ticker or exchange code — verify with eodhd_search first."""
    params = EodhdEodParams(ticker=ticker, from_date=from_date, to_date=to_date, period=period)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"ticker": params.ticker}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    if params.period:
        p["period"] = params.period
    return await _eodhd_fetch(http, path="/eod/{ticker}", params=p, op_name="eodhd_eod", output_config=_EOD_OUTPUT)


@connector(output=_LIVE_OUTPUT, tags=["eodhd", "equity", "tool"])
async def eodhd_live(ticker: Annotated[str, 'ns:eodhd_symbols'], *, api_key: str) -> Any:
    """[Free+] Fetch live (real-time or 15-min delayed) quote for a ticker. Use eodhd_search
    to resolve a company name to its EODHD ticker format (e.g. AAPL.US)."""
    params = EodhdLiveParams(ticker=ticker)  # type: ignore[call-arg]
    http = _make_http(api_key, timeout=_LATENCY_TIMEOUT)
    return await _eodhd_fetch(
        http,
        path="/real-time/{ticker}",
        params={"ticker": params.ticker},
        op_name="eodhd_live",
        output_config=_LIVE_OUTPUT,
    )


@connector(output=_INTRADAY_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_intraday(
    ticker: Annotated[str, 'ns:eodhd_symbols'],
    interval: Literal['1m', '5m', '1h'],
    from_unix: int | None = None,
    to_unix: int | None = None,
    *,
    api_key: str
) -> Any:
    """[EOD+Intraday+] Fetch intraday OHLCV data for a ticker. Intervals: 1m, 5m, 1h.
    Provide from_unix / to_unix as Unix timestamps (seconds) to bound the range.
    Returns at most the last 100 data points when no range is specified."""
    params = EodhdIntradayParams(ticker=ticker, interval=interval, from_unix=from_unix, to_unix=to_unix)  # type: ignore[call-arg]
    http = _make_http(api_key, timeout=_LATENCY_TIMEOUT)
    p: dict[str, Any] = {"ticker": params.ticker, "interval": params.interval}
    if params.from_unix is not None:
        p["from"] = params.from_unix
    if params.to_unix is not None:
        p["to"] = params.to_unix
    return await _eodhd_fetch(
        http, path="/intraday/{ticker}", params=p, op_name="eodhd_intraday", output_config=_INTRADAY_OUTPUT
    )


@connector(output=_BULK_EOD_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_bulk_eod(exchange: str, date: str | None = None, *, api_key: str) -> Any:
    """[EOD Historical+] Fetch end-of-day prices for all symbols on an exchange in a single request.
    Returns the last trading day by default; pass date to fetch a specific day.
    Large response — use for batch ingestion, not per-ticker lookups."""
    params = EodhdBulkEodParams(exchange=exchange, date=date)  # type: ignore[call-arg]
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    p: dict[str, Any] = {"exchange": params.exchange}
    if params.date:
        p["date"] = params.date
    return await _eodhd_fetch(
        http, path="/eod/bulk_last_day/{exchange}", params=p, op_name="eodhd_bulk_eod", output_config=_BULK_EOD_OUTPUT
    )


# ---------------------------------------------------------------------------
# Corporate Actions — Connectors
# ---------------------------------------------------------------------------


@connector(output=_DIVIDENDS_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_dividends(
    ticker: Annotated[str, 'ns:eodhd_symbols'],
    from_date: str | None = None,
    to_date: str | None = None,
    *,
    api_key: str
) -> Any:
    """[Free+] Fetch dividend history for a ticker. Use from/to to limit the range."""
    params = EodhdDividendsParams(ticker=ticker, from_date=from_date, to_date=to_date)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"ticker": params.ticker}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/div/{ticker}", params=p, op_name="eodhd_dividends", output_config=_DIVIDENDS_OUTPUT
    )


@connector(output=_SPLITS_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_splits(
    ticker: Annotated[str, 'ns:eodhd_symbols'],
    from_date: str | None = None,
    to_date: str | None = None,
    *,
    api_key: str
) -> Any:
    """[Free+] Fetch stock split history for a ticker. The split ratio column contains the
    ratio string as returned by the API (e.g. "4/1" for a 4-for-1 split). Use from/to to limit the range."""
    params = EodhdSplitsParams(ticker=ticker, from_date=from_date, to_date=to_date)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"ticker": params.ticker}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/splits/{ticker}", params=p, op_name="eodhd_splits", output_config=_SPLITS_OUTPUT
    )


# ---------------------------------------------------------------------------
# Reference Data — Connectors
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["eodhd", "tool"])
async def eodhd_search(
    query: str,
    limit: int = 50,
    type: Literal['Q', 'ETF', 'FUND', 'BOND', 'INDEX'] | None = None,
    *,
    api_key: str
) -> Any:
    """[Free+] Search for instruments by company name or partial ticker. Use to resolve company
    names to EODHD ticker codes (format: TICKER.EXCHANGE, e.g. AAPL.US). Filter by type to
    narrow results."""
    params = EodhdSearchParams(query=query, limit=limit, type=type)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"query": params.query, "limit": params.limit}
    if params.type:
        p["type"] = params.type
    return await _eodhd_fetch(
        http, path="/search/{query}", params=p, op_name="eodhd_search", output_config=_SEARCH_OUTPUT
    )


@connector(output=_EXCHANGES_OUTPUT, tags=["eodhd", "tool"])
async def eodhd_exchanges(*, api_key: str) -> Any:
    """[Free+] List all exchanges supported by EODHD. Use to find valid exchange codes for
    eodhd_bulk_eod and eodhd_exchange_symbols."""
    http = _make_http(api_key)
    return await _eodhd_fetch(
        http, path="/exchanges-list", params={}, op_name="eodhd_exchanges", output_config=_EXCHANGES_OUTPUT
    )


@connector(output=_EXCHANGE_SYMBOLS_OUTPUT, tags=["eodhd"])
async def eodhd_exchange_symbols(
    exchange: str,
    type: Literal['common_stock', 'preferred_stock', 'stock', 'etf', 'fund'] | None = None,
    *,
    api_key: str
) -> Any:
    """[Free+] List all symbols traded on an exchange. Large response for major exchanges
    (US has 20 000+ symbols) — use type filter to limit. Empty result may indicate an
    invalid exchange code."""
    params = EodhdExchangeSymbolsParams(exchange=exchange, type=type)  # type: ignore[call-arg]
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    p: dict[str, Any] = {"exchange": params.exchange}
    if params.type:
        p["type"] = params.type
    return await _eodhd_fetch(
        http,
        path="/exchange-symbol-list/{exchange}",
        params=p,
        op_name="eodhd_exchange_symbols",
        output_config=_EXCHANGE_SYMBOLS_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Fundamentals — Connector
# ---------------------------------------------------------------------------


@connector(tags=["eodhd", "equity"])
async def eodhd_fundamentals(ticker: Annotated[str, 'ns:eodhd_symbols'], *, api_key: str) -> Any:
    """[Fundamentals+] Fetch full fundamentals for a stock or ETF. Returns a large nested dict
    (not a DataFrame). Typical top-level keys for equities: General, Highlights, Valuation,
    SharesStats, Technicals, SplitsDividends, AnalystRatings, Holders, InsiderTransactions,
    Financials, Earnings. ETF top-level keys differ: General, Technicals, ETF_Data.

    Navigate by key path, e.g.:
      result.data['Highlights']['MarketCapitalization']
      result.data['Financials']['Income_Statement']['annual']

    Returns raw dict — use result.data to access the nested structure."""
    params = EodhdFundamentalsParams(ticker=ticker)  # type: ignore[call-arg]
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    return await _eodhd_fetch(
        http,
        path="/fundamentals/{ticker}",
        params={"ticker": params.ticker},
        op_name="eodhd_fundamentals",
        raw=True,
    )


# ---------------------------------------------------------------------------
# Calendars — Dispatch map + Connector
# ---------------------------------------------------------------------------

_CALENDAR_PATHS: dict[str, str] = {
    "earnings": "calendar/earnings",
    "ipo": "calendar/ipo",
    "trends": "calendar/trends",
}


@connector(output=_CALENDAR_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_calendar(
    type: Literal['earnings', 'ipo', 'trends'],
    from_date: str | None = None,
    to_date: str | None = None,
    symbols: str | None = None,
    *,
    api_key: str
) -> Any:
    """[Fundamentals+] Fetch market calendar data. Three types available:
      - earnings: upcoming earnings announcements with EPS estimates and actuals
      - ipo: upcoming and recent IPO listings
      - trends: analyst recommendation trends by sector

    Use from/to to narrow the date window (max 90 days recommended for earnings)."""
    params = EodhdCalendarParams(type=type, from_date=from_date, to_date=to_date, symbols=symbols)  # type: ignore[call-arg]
    http = _make_http(api_key)
    path = _CALENDAR_PATHS[params.type]
    p: dict[str, Any] = {}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    if params.symbols:
        p["symbols"] = params.symbols
    return await _eodhd_fetch(http, path=path, params=p, op_name="eodhd_calendar", output_config=_CALENDAR_OUTPUT)


# ---------------------------------------------------------------------------
# News — Connector
# ---------------------------------------------------------------------------


@connector(output=_NEWS_OUTPUT, tags=["eodhd", "tool"])
async def eodhd_news(
    ticker: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 50,
    offset: int = 0,
    *,
    api_key: str
) -> Any:
    """[Free+] Fetch financial news articles. Filter by ticker (e.g. AAPL.US) or leave
    empty for broad market news. Use from/to for date filtering and limit/offset for pagination.
    Empty result may indicate no news in the date range for the specified ticker."""
    params = EodhdNewsParams(ticker=ticker, from_date=from_date, to_date=to_date, limit=limit, offset=offset)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"limit": params.limit, "offset": params.offset}
    if params.ticker:
        p["s"] = params.ticker  # EODHD uses 's=' for symbol filtering on news endpoint
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(http, path="/news", params=p, op_name="eodhd_news", output_config=_NEWS_OUTPUT)


# ---------------------------------------------------------------------------
# Macro Indicators — Connectors
# ---------------------------------------------------------------------------


@connector(output=_MACRO_OUTPUT, tags=["eodhd", "macro"])
async def eodhd_macro(country: str, indicator: str, *, api_key: str) -> Any:
    """[Fundamentals+] Fetch a macro indicator time series for a country.
    Country must be an ISO 3-letter code (e.g. USA, DEU). Common indicators:
      gdp_current_usd, unemployment_total_percent, inflation_consumer_prices_annual,
      real_interest_rate, population_total, exports_of_goods_and_services_usd."""
    params = EodhdMacroParams(country=country, indicator=indicator)  # type: ignore[call-arg]
    http = _make_http(api_key)
    return await _eodhd_fetch(
        http,
        path="/macro-indicator/{country}",
        params={"country": params.country, "indicator": params.indicator},
        op_name="eodhd_macro",
        output_config=_MACRO_OUTPUT,
    )


@connector(output=_MACRO_OUTPUT, tags=["eodhd", "macro"])
async def eodhd_macro_bulk(country: str, topic: str | None = None, *, api_key: str) -> Any:
    """[Fundamentals+] Fetch all available macro indicators for a country in a single request.
    Large response — use eodhd_macro for a specific indicator.
    Country must be an ISO 3-letter code (e.g. USA)."""
    params = EodhdMacroBulkParams(country=country, topic=topic)  # type: ignore[call-arg]
    http = _make_http(api_key, timeout=_BULK_TIMEOUT)
    p: dict[str, Any] = {"country": params.country}
    if params.topic:
        p["topic"] = params.topic
    return await _eodhd_fetch(
        http, path="/macro-indicator/{country}", params=p, op_name="eodhd_macro_bulk", output_config=_MACRO_OUTPUT
    )


# ---------------------------------------------------------------------------
# Technical Indicators — Connector
# ---------------------------------------------------------------------------


@connector(output=_TECHNICAL_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_technical(
    ticker: Annotated[str, 'ns:eodhd_symbols'],
    function: _EodhdTechnicalFunction,
    period: int = 50,
    from_date: str | None = None,
    to_date: str | None = None,
    order: Literal['a', 'd'] = 'd',
    *,
    api_key: str
) -> Any:
    """[EOD+Intraday+] Fetch technical indicator values for a ticker alongside OHLCV data.
    Indicator-specific output columns vary by function:
      - sma/ema/wma → sma/ema/wma column
      - macd → macd, macd_signal, macd_hist
      - bbands → uband, mband, lband
      - stochastic → stoch_kd, stoch_d
      - adx/dmi → adx, plusDI, minusDI

    Use period to control the lookback window (default 50)."""
    params = EodhdTechnicalParams(
        ticker=ticker,
        function=function,
        period=period,
        from_date=from_date,
        to_date=to_date,
        order=order)  # type: ignore[call-arg]

    http = _make_http(api_key)
    p: dict[str, Any] = {
        "ticker": params.ticker,
        "function": params.function,
        "period": params.period,
        "order": params.order,
    }
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await _eodhd_fetch(
        http, path="/technicals/{ticker}", params=p, op_name="eodhd_technical", output_config=_TECHNICAL_OUTPUT
    )


# ---------------------------------------------------------------------------
# Insider Transactions & Screener — Connectors
# ---------------------------------------------------------------------------


@connector(output=_INSIDER_OUTPUT, tags=["eodhd", "equity"])
async def eodhd_insider(
    ticker: Annotated[str, 'ns:eodhd_symbols'] | None = None,
    limit: int = 100,
    offset: int = 0,
    *,
    api_key: str
) -> Any:
    """[Fundamentals+] Fetch insider (executive and director) transactions. Filter by ticker
    or omit for recent cross-market transactions. Use limit/offset to page."""
    params = EodhdInsiderParams(ticker=ticker, limit=limit, offset=offset)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"limit": params.limit, "offset": params.offset}
    if params.ticker:
        p["code"] = params.ticker
    return await _eodhd_fetch(
        http, path="/insider-transactions", params=p, op_name="eodhd_insider", output_config=_INSIDER_OUTPUT
    )


@connector(output=_SCREENER_OUTPUT, tags=["eodhd", "equity", "tool"])
async def eodhd_screener(
    filters: list[tuple[str, str, str]] | None = None,
    signals: str | None = None,
    sort: str | None = None,
    order: Literal['asc', 'desc'] = 'desc',
    limit: int = 50,
    offset: int = 0,
    *,
    api_key: str
) -> Any:
    """[EOD+Intraday+] Screen stocks by fundamental, price, and exchange criteria.
    Filters are structured triples [field, operator, value] — see EodhdScreenerParams.filters.
    Empty result may indicate invalid filter field or operator — verify against the EODHD
    screener field list in their documentation."""
    params = EodhdScreenerParams(filters=filters, signals=signals, sort=sort, order=order, limit=limit, offset=offset)  # type: ignore[call-arg]
    http = _make_http(api_key)
    p: dict[str, Any] = {"limit": params.limit, "offset": params.offset, "order": params.order}
    if params.filters:
        p["filters"] = json.dumps([[f[0], f[1], f[2]] for f in params.filters])
    if params.signals:
        p["signals"] = params.signals
    if params.sort:
        p["sort"] = params.sort
    return await _eodhd_fetch(
        http, path="/screener", params=p, op_name="eodhd_screener", output_config=_SCREENER_OUTPUT
    )


# ---------------------------------------------------------------------------
# Connector collections
# ---------------------------------------------------------------------------

CONNECTORS = Connectors(
    [
        # Discovery
        eodhd_search,
        eodhd_exchanges,
        eodhd_news,
        eodhd_screener,
        # Market data
        eodhd_eod,
        eodhd_live,
        eodhd_intraday,
        eodhd_bulk_eod,
        # Corporate actions
        eodhd_dividends,
        eodhd_splits,
        # Reference
        eodhd_exchange_symbols,
        # Fundamentals
        eodhd_fundamentals,
        # Calendars
        eodhd_calendar,
        # Macro
        eodhd_macro,
        eodhd_macro_bulk,
        # Technical
        eodhd_technical,
        # Transactions
        eodhd_insider,
    ]
)


__all__ = ["CONNECTORS"]
