"""EODHD source: end-of-day, intraday, fundamentals, news, calendars, macro.

API docs: https://eodhd.com/financial-apis/
Authentication: ``api_token`` query parameter (in the transport sensitive-param
  set, so it is redacted from every log line and never reaches a surfaced URL).
Base URL: https://eodhd.com/api

Provides 17 plain ``@connector`` verbs over the EODHD REST surface:
  - Market data: EOD prices, live quotes, intraday, bulk EOD
  - Corporate actions: dividends, splits
  - Reference: search, exchanges, exchange symbol lists
  - Fundamentals (raw nested dict)
  - Calendars: earnings, IPO, trends, splits
  - News
  - Macro indicators (single + bulk)
  - Technical indicators
  - Insider transactions
  - Screener

The API key is declared as a secret (stripped from provenance) and supplied
either by binding (``load(api_key=...)`` / ``Connector.bind``) or, as a dev
fallback, from the ``EODHD_API_KEY`` environment variable. A missing key fails
fast with :class:`UnauthorizedError` naming the env var.

Status semantics (verified live 2026-06-04): an invalid key returns 401
(→ :class:`UnauthorizedError`); a plan-restricted endpoint returns 403, and a
bulk plan-restriction returns 423 Locked — both surfaced as
:class:`PaymentRequiredError`. Many verbs require a paid EODHD plan; their
docstrings tag the minimum plan as ``[Free+]``, ``[EOD+Intraday+]``, or
``[Fundamentals+]`` and they return :class:`PaymentRequiredError` on a free key.

Internal layout (not part of the public contract):

* :mod:`parsimony_eodhd._http` — keyed client builder and error mapping.
* :mod:`parsimony_eodhd.outputs` — declarative :class:`OutputSpec` schemas.
"""

from __future__ import annotations

import json
import re
from typing import Annotated, Any, Literal

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_eodhd._http import _client, eodhd_get
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

__all__ = ["CONNECTORS", "load"]

_PROVIDER = "eodhd"

_LATENCY_TIMEOUT: float = 10.0
_BULK_TIMEOUT: float = 60.0

# Technical-indicator function names accepted by eodhd_technical.
_TechnicalFunction = Literal[
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

# Guard for values interpolated directly into request paths
# (``/eod/<ticker>``, ``/exchange-symbol-list/<exchange>`` etc.).
_PATH_TOKEN_RE = re.compile(r"^[A-Za-z0-9._\-]+$")


def _safe_path_token(value: str, name: str) -> str:
    """Validate and return a value that is interpolated into a request path."""
    cleaned = value.strip()
    if not cleaned:
        raise InvalidParameterError(_PROVIDER, f"{name} must be non-empty")
    if not _PATH_TOKEN_RE.match(cleaned):
        raise InvalidParameterError(_PROVIDER, f"{name} contains unsafe characters for a URL path: {value!r}")
    return cleaned


def _select_declared(df: pd.DataFrame, output: Any) -> pd.DataFrame:
    """Project a frame to the columns the schema declares, in declared order.

    Drops provider extras not in the schema. Missing declared columns are filled
    with ``NA`` so :class:`~parsimony.result.OutputSpec` can shape sparse
    payloads (calendar types, dividend-adjusted prices, etc.) without folding
    extras in as stray DATA columns. Wildcard (``"*"``) schemas keep unmapped
    columns after the fixed prefix.
    """
    names = [c.name for c in output.columns]
    fixed = [n for n in names if n != "*"]
    out = df.copy()
    for n in fixed:
        if n not in out.columns:
            out[n] = pd.NA
    if "*" in names:
        extra = [c for c in out.columns if c not in fixed]
        return out[fixed + extra]
    return out[fixed]


def _rows_to_frame(data: Any, op_name: str, query_params: dict[str, Any]) -> pd.DataFrame:
    """Build a DataFrame from a JSON list of records; guard empty/parse failures."""
    if not isinstance(data, list):
        raise ParseError(_PROVIDER, f"{op_name} response was not a JSON array")
    if not data:
        raise EmptyDataError(_PROVIDER, query_params=query_params)
    df = pd.DataFrame(data)
    if df.empty:
        raise EmptyDataError(_PROVIDER, query_params=query_params)
    return df


# ---------------------------------------------------------------------------
# Reference Data — search / exchanges / exchange symbols
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["eodhd", "tool"], secrets=("api_key",))
def eodhd_search(
    query: str,
    limit: int = 50,
    type: Literal["stock", "etf", "fund", "bond", "index"] | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Free+] Search EODHD for instruments by company name or partial ticker.
    Resolves names to EODHD ticker codes (format TICKER.EXCHANGE, e.g. AAPL.US).
    Returns Code, Name, Exchange, Type, Country, Currency, ISIN. Optionally
    filter by type: stock, etf, fund, bond, index.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError(_PROVIDER, "query must be non-empty")
    if limit < 1 or limit > 500:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 500")

    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"search/{q}",
        params={"limit": limit, "type": type},
        op_name="eodhd_search",
    )
    df = _rows_to_frame(data, "eodhd_search", {"query": q})
    return _select_declared(df, _SEARCH_OUTPUT)


@connector(output=_EXCHANGES_OUTPUT, tags=["eodhd", "tool"], secrets=("api_key",))
def eodhd_exchanges(api_key: str = "") -> pd.DataFrame:
    """[Free+] List all exchanges supported by EODHD. Returns Code, Name, country,
    currency and operating MIC. Use the Code with eodhd_bulk_eod and
    eodhd_exchange_symbols.
    """
    http = _client(api_key)
    data = eodhd_get(http, path="exchanges-list", op_name="eodhd_exchanges")
    df = _rows_to_frame(data, "eodhd_exchanges", {})
    return _select_declared(df, _EXCHANGES_OUTPUT)


@connector(output=_EXCHANGE_SYMBOLS_OUTPUT, tags=["eodhd"], secrets=("api_key",))
def eodhd_exchange_symbols(
    exchange: str,
    type: Literal["common_stock", "preferred_stock", "stock", "etf", "fund"] | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Free+] List all symbols traded on an exchange (returns Code, Name, country,
    exchange, currency, type, ISIN). Large response for major exchanges (US has
    50 000+ symbols) — use the type filter to narrow. Empty result may indicate
    an invalid exchange code.
    """
    ex = _safe_path_token(exchange, "exchange")
    http = _client(api_key, timeout=_BULK_TIMEOUT)
    data = eodhd_get(
        http,
        path=f"exchange-symbol-list/{ex}",
        params={"type": type},
        op_name="eodhd_exchange_symbols",
    )
    df = _rows_to_frame(data, "eodhd_exchange_symbols", {"exchange": ex})
    return _select_declared(df, _EXCHANGE_SYMBOLS_OUTPUT)


# ---------------------------------------------------------------------------
# Market Data — EOD / live / intraday / bulk
# ---------------------------------------------------------------------------


@connector(output=_EOD_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_eod(
    ticker: Annotated[str, Namespace("eodhd_symbols")],
    from_date: str | None = None,
    to_date: str | None = None,
    period: Literal["d", "w", "m"] | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Free+] Fetch end-of-day OHLCV prices for a ticker. Supports daily, weekly,
    and monthly aggregation via period. Use from_date/to_date (ISO 8601) to limit
    the range. Empty result may indicate an invalid ticker or exchange code —
    verify with eodhd_search first. Free tier is limited to ~1 year of history.
    """
    t = _safe_path_token(ticker, "ticker")
    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"eod/{t}",
        params={"from": from_date, "to": to_date, "period": period},
        op_name="eodhd_eod",
    )
    df = _rows_to_frame(data, "eodhd_eod", {"ticker": t})
    df = _select_declared(df, _EOD_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


@connector(output=_LIVE_OUTPUT, tags=["eodhd", "equity", "tool"], secrets=("api_key",))
def eodhd_live(ticker: Annotated[str, Namespace("eodhd_symbols")], api_key: str = "") -> pd.DataFrame:
    """[Free+] Fetch the live (real-time or 15-min delayed) quote for a ticker:
    code, timestamp, OHLC, volume, previous close, and change. Use eodhd_search
    to resolve a company name to its EODHD ticker (e.g. AAPL.US).
    """
    t = _safe_path_token(ticker, "ticker")
    http = _client(api_key, timeout=_LATENCY_TIMEOUT)
    data = eodhd_get(http, path=f"real-time/{t}", op_name="eodhd_live")
    if not isinstance(data, dict) or not data.get("code"):
        raise EmptyDataError(_PROVIDER, query_params={"ticker": t})
    df = pd.DataFrame([data])
    return _select_declared(df, _LIVE_OUTPUT)


@connector(output=_INTRADAY_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_intraday(
    ticker: Annotated[str, Namespace("eodhd_symbols")],
    interval: Literal["1m", "5m", "1h"],
    from_unix: int | None = None,
    to_unix: int | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[EOD+Intraday+] Fetch intraday OHLCV data for a ticker at 1m, 5m, or 1h
    intervals. Provide from_unix / to_unix (Unix timestamps in seconds) to bound
    the range; otherwise the most recent points are returned. Requires a paid
    EOD+Intraday plan — a free key returns PaymentRequiredError.
    """
    t = _safe_path_token(ticker, "ticker")
    http = _client(api_key, timeout=_LATENCY_TIMEOUT)
    data = eodhd_get(
        http,
        path=f"intraday/{t}",
        params={"interval": interval, "from": from_unix, "to": to_unix},
        op_name="eodhd_intraday",
    )
    df = _rows_to_frame(data, "eodhd_intraday", {"ticker": t})
    df = _select_declared(df, _INTRADAY_OUTPUT)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    return df


@connector(output=_BULK_EOD_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_bulk_eod(exchange: str, date: str | None = None, api_key: str = "") -> pd.DataFrame:
    """[EOD Historical+] Fetch end-of-day prices for every symbol on an exchange
    in one request (returns code, name, date, OHLCV). Defaults to the last
    trading day; pass date to fetch a specific day. Large response — use for
    batch ingestion, not per-ticker lookups. Requires a paid plan; a free key
    returns PaymentRequiredError.
    """
    ex = _safe_path_token(exchange, "exchange")
    http = _client(api_key, timeout=_BULK_TIMEOUT)
    data = eodhd_get(
        http,
        path=f"eod-bulk-last-day/{ex}",
        params={"date": date},
        op_name="eodhd_bulk_eod",
    )
    df = _rows_to_frame(data, "eodhd_bulk_eod", {"exchange": ex})
    df = _select_declared(df, _BULK_EOD_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Corporate Actions — dividends / splits
# ---------------------------------------------------------------------------


@connector(output=_DIVIDENDS_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_dividends(
    ticker: Annotated[str, Namespace("eodhd_symbols")],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Free+] Fetch dividend history for a ticker: ex-date, declaration/record/
    payment dates, period, value, unadjusted value, and currency. Use from_date/
    to_date (ISO 8601) to limit the range.
    """
    t = _safe_path_token(ticker, "ticker")
    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"div/{t}",
        params={"from": from_date, "to": to_date},
        op_name="eodhd_dividends",
    )
    df = _rows_to_frame(data, "eodhd_dividends", {"ticker": t})
    df = _select_declared(df, _DIVIDENDS_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["declarationDate"] = pd.to_datetime(df["declarationDate"]).dt.normalize()
    df["recordDate"] = pd.to_datetime(df["recordDate"]).dt.normalize()
    df["paymentDate"] = pd.to_datetime(df["paymentDate"]).dt.normalize()
    return df


@connector(output=_SPLITS_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_splits(
    ticker: Annotated[str, Namespace("eodhd_symbols")],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Free+] Fetch stock split history for a ticker. The split column carries the
    ratio string as returned by the API (e.g. "2.000000/1.000000" for a 2-for-1
    split). Use from_date/to_date (ISO 8601) to limit the range.
    """
    t = _safe_path_token(ticker, "ticker")
    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"splits/{t}",
        params={"from": from_date, "to": to_date},
        op_name="eodhd_splits",
    )
    df = _rows_to_frame(data, "eodhd_splits", {"ticker": t})
    df = _select_declared(df, _SPLITS_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Fundamentals — raw nested dict (no output schema)
# ---------------------------------------------------------------------------


@connector(tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_fundamentals(ticker: Annotated[str, Namespace("eodhd_symbols")], api_key: str = "") -> dict[str, Any]:
    """[Fundamentals+] Fetch full fundamentals for a stock or ETF as a large nested
    dict (not a DataFrame). Typical equity top-level keys: General, Highlights,
    Valuation, SharesStats, Technicals, SplitsDividends, AnalystRatings, Holders,
    InsiderTransactions, Financials, Earnings. ETFs differ (General, Technicals,
    ETF_Data). Navigate via result.data, e.g.
    result.data['Highlights']['MarketCapitalization']. Requires a paid plan; a
    free key returns PaymentRequiredError.
    """
    t = _safe_path_token(ticker, "ticker")
    http = _client(api_key, timeout=_BULK_TIMEOUT)
    data = eodhd_get(http, path=f"fundamentals/{t}", op_name="eodhd_fundamentals")
    if not isinstance(data, dict) or not data:
        raise EmptyDataError(_PROVIDER, query_params={"ticker": t})
    return data


# ---------------------------------------------------------------------------
# Calendars — earnings / ipos / trends / splits
# ---------------------------------------------------------------------------


@connector(output=_CALENDAR_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_calendar(
    type: Literal["earnings", "ipos", "trends", "splits"],
    from_date: str | None = None,
    to_date: str | None = None,
    symbols: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Fundamentals+] Fetch market calendar data. Types: earnings (upcoming EPS
    announcements with estimates/actuals), ipos (upcoming and recent IPO
    listings), trends (analyst recommendation trends), splits (upcoming splits).
    Use from_date/to_date to narrow the window and symbols to filter. Requires a
    paid plan; a free key returns PaymentRequiredError.
    """
    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"calendar/{type}",
        params={"from": from_date, "to": to_date, "symbols": symbols},
        op_name="eodhd_calendar",
    )
    # The calendar endpoints wrap rows under a type-specific key.
    if isinstance(data, dict):
        for key in ("earnings", "ipos", "trends", "splits", "data"):
            rows = data.get(key)
            if isinstance(rows, list):
                data = rows
                break
    df = _rows_to_frame(data, "eodhd_calendar", {"type": type})
    df = _select_declared(df, _CALENDAR_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["report_date"] = pd.to_datetime(df["report_date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------


@connector(output=_NEWS_OUTPUT, tags=["eodhd", "tool"], secrets=("api_key",))
def eodhd_news(
    ticker: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 50,
    offset: int = 0,
    api_key: str = "",
) -> pd.DataFrame:
    """[Free+] Fetch financial news articles (date, title, content, link, related
    symbols, tags). Filter by ticker (e.g. AAPL.US) or omit for broad market
    news. Use from_date/to_date for date filtering and limit/offset to page.
    Empty result may indicate no news in the range for the ticker.
    """
    if limit < 1 or limit > 1000:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 1000")
    if offset < 0:
        raise InvalidParameterError(_PROVIDER, "offset must be non-negative")

    http = _client(api_key)
    data = eodhd_get(
        http,
        path="news",
        params={
            "s": ticker,  # EODHD uses ``s=`` for symbol filtering on the news endpoint
            "from": from_date,
            "to": to_date,
            "limit": limit,
            "offset": offset,
        },
        op_name="eodhd_news",
    )
    df = _rows_to_frame(data, "eodhd_news", {"ticker": ticker or ""})
    df = _select_declared(df, _NEWS_OUTPUT)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------------------------------------------------------------------------
# Macro indicators — single / bulk
# ---------------------------------------------------------------------------


@connector(output=_MACRO_OUTPUT, tags=["eodhd", "macro"], secrets=("api_key",))
def eodhd_macro(country: str, indicator: str, api_key: str = "") -> pd.DataFrame:
    """[Fundamentals+] Fetch a macro indicator time series for a country (returns
    Date, Value, Period). Country must be an ISO 3-letter code (e.g. USA, DEU).
    Common indicators: gdp_current_usd, unemployment_total_percent,
    inflation_consumer_prices_annual, real_interest_rate, population_total.
    Requires a paid plan; a free key returns PaymentRequiredError.
    """
    c = _safe_path_token(country, "country")
    ind = indicator.strip()
    if not ind:
        raise InvalidParameterError(_PROVIDER, "indicator must be non-empty")
    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"macro-indicator/{c}",
        params={"indicator": ind},
        op_name="eodhd_macro",
    )
    df = _rows_to_frame(data, "eodhd_macro", {"country": c, "indicator": ind})
    df = _select_declared(df, _MACRO_OUTPUT)
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    return df


@connector(output=_MACRO_OUTPUT, tags=["eodhd", "macro"], secrets=("api_key",))
def eodhd_macro_bulk(country: str, indicator: str | None = None, api_key: str = "") -> pd.DataFrame:
    """[Fundamentals+] Fetch macro indicator data for a country. The EODHD
    macro-indicator endpoint requires an indicator; pass one explicitly or rely
    on the default. Returns Date, Value, Period. Country must be an ISO 3-letter
    code (e.g. USA). Requires a paid plan; a free key returns
    PaymentRequiredError.
    """
    c = _safe_path_token(country, "country")
    http = _client(api_key, timeout=_BULK_TIMEOUT)
    data = eodhd_get(
        http,
        path=f"macro-indicator/{c}",
        params={"indicator": indicator},
        op_name="eodhd_macro_bulk",
    )
    df = _rows_to_frame(data, "eodhd_macro_bulk", {"country": c})
    df = _select_declared(df, _MACRO_OUTPUT)
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Technical indicators
# ---------------------------------------------------------------------------


@connector(output=_TECHNICAL_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_technical(
    ticker: Annotated[str, Namespace("eodhd_symbols")],
    function: _TechnicalFunction,
    period: int = 50,
    from_date: str | None = None,
    to_date: str | None = None,
    order: Literal["a", "d"] = "d",
    api_key: str = "",
) -> pd.DataFrame:
    """[EOD+Intraday+] Fetch technical indicator values for a ticker. The output
    columns vary by function: sma/ema/wma → the indicator column; macd → macd,
    macd_signal, macd_hist; bbands → uband, mband, lband; stochastic → k/d;
    adx/dmi → adx, plusDI, minusDI. Use period for the lookback window (default
    50). Requires a paid plan; a free key returns PaymentRequiredError.
    """
    t = _safe_path_token(ticker, "ticker")
    if period < 1:
        raise InvalidParameterError(_PROVIDER, "period must be a positive integer")
    http = _client(api_key)
    data = eodhd_get(
        http,
        path=f"technical/{t}",
        params={
            "function": function,
            "period": period,
            "order": order,
            "from": from_date,
            "to": to_date,
        },
        op_name="eodhd_technical",
    )
    df = _rows_to_frame(data, "eodhd_technical", {"ticker": t, "function": function})
    df = _select_declared(df, _TECHNICAL_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Insider transactions
# ---------------------------------------------------------------------------


@connector(output=_INSIDER_OUTPUT, tags=["eodhd", "equity"], secrets=("api_key",))
def eodhd_insider(
    ticker: Annotated[str, Namespace("eodhd_symbols")] | None = None,
    limit: int = 100,
    offset: int = 0,
    api_key: str = "",
) -> pd.DataFrame:
    """[Fundamentals+] Fetch insider (executive and director) transactions. Filter
    by ticker (e.g. AAPL.US) or omit for recent cross-market transactions. Use
    limit/offset to page. Requires a paid plan; a free key returns
    PaymentRequiredError.
    """
    if limit < 1 or limit > 1000:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 1000")
    if offset < 0:
        raise InvalidParameterError(_PROVIDER, "offset must be non-negative")
    code = ticker.strip() if ticker else None
    http = _client(api_key)
    data = eodhd_get(
        http,
        path="insider-transactions",
        params={"code": code, "limit": limit, "offset": offset},
        op_name="eodhd_insider",
    )
    df = _rows_to_frame(data, "eodhd_insider", {"ticker": code or ""})
    df = _select_declared(df, _INSIDER_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["transactionDate"] = pd.to_datetime(df["transactionDate"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Screener
# ---------------------------------------------------------------------------


@connector(output=_SCREENER_OUTPUT, tags=["eodhd", "equity", "tool"], secrets=("api_key",))
def eodhd_screener(
    filters: list[tuple[str, str, str]] | None = None,
    signals: str | None = None,
    sort: str | None = None,
    order: Literal["asc", "desc"] = "desc",
    limit: int = 50,
    offset: int = 0,
    api_key: str = "",
) -> pd.DataFrame:
    """[EOD+Intraday+] Screen stocks by fundamental, price, and exchange criteria.
    filters is a list of [field, operator, value] triples, e.g.
    [["market_capitalization", ">", "1000000000"], ["exchange", "=", "us"]].
    Operators: >, <, =, >=, <=. Common fields: market_capitalization,
    earnings_share, dividend_yield, refund_1d_p, sector, exchange. Requires a
    paid plan; a free key returns PaymentRequiredError.
    """
    if limit < 1 or limit > 100:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 100")
    if offset < 0:
        raise InvalidParameterError(_PROVIDER, "offset must be non-negative")

    params: dict[str, Any] = {"limit": limit, "offset": offset, "sort": sort, "signals": signals}
    if filters:
        params["filters"] = json.dumps([[f[0], f[1], f[2]] for f in filters])
    if sort and order:
        # EODHD sort syntax is ``field.direction`` (e.g. market_capitalization.desc).
        params["sort"] = f"{sort}.{order}"

    http = _client(api_key)
    data = eodhd_get(http, path="screener", params=params, op_name="eodhd_screener")
    # The screener wraps rows under a top-level ``data`` key.
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            data = rows
    df = _rows_to_frame(data, "eodhd_screener", {"limit": limit})
    return df


# ---------------------------------------------------------------------------
# Connector collection
# ---------------------------------------------------------------------------

CONNECTORS = Connectors(
    [
        # Reference / discovery
        eodhd_search,
        eodhd_exchanges,
        eodhd_exchange_symbols,
        # Market data
        eodhd_eod,
        eodhd_live,
        eodhd_intraday,
        eodhd_bulk_eod,
        # Corporate actions
        eodhd_dividends,
        eodhd_splits,
        # Fundamentals
        eodhd_fundamentals,
        # Calendars
        eodhd_calendar,
        # News
        eodhd_news,
        # Macro
        eodhd_macro,
        eodhd_macro_bulk,
        # Technical
        eodhd_technical,
        # Transactions
        eodhd_insider,
        # Screener
        eodhd_screener,
    ]
)


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
