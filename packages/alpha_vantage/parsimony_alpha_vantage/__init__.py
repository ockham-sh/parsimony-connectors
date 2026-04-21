"""Alpha Vantage source: equities, fundamentals, forex, crypto, commodities, economic indicators, and news.

API docs: https://www.alphavantage.co/documentation/
Authentication: ``apikey`` query parameter.
Base URL: https://www.alphavantage.co/query
Rate limit: 25 calls/day, 5 calls/min (free tier).

Quirks:
  - Single endpoint (``/query``), differentiated by ``function`` param.
  - Errors return HTTP 200 with ``Error Message``, ``Note``, or ``Information`` keys.
  - Many JSON field names are numbered (``"1. open"``) — stripped during parsing.
  - All values are strings — numeric coercion required.
  - Missing data represented as ``"None"`` (string) or ``"."`` (commodities/economic).

Provides 29 connectors (28 ``@connector`` + 1 ``@enumerator``):
  - Discovery: symbol search
  - Market data: real-time quote, daily/weekly/monthly/intraday OHLCV
  - Company: overview, income statement, balance sheet, cash flow, earnings, ETF profile
  - Calendars: earnings calendar, IPO calendar (CSV endpoints)
  - Forex: real-time exchange rate, daily/weekly/monthly historical
  - Crypto: daily/weekly/monthly historical
  - Economic indicators: 10 US macro series (GDP, CPI, unemployment, etc.)
  - Precious metals: gold/silver spot price and historical (real-time, not in FRED)
  - Technical indicators: 50+ indicators via unified endpoint (SMA, EMA, RSI, MACD, etc.)
  - Alpha intelligence: news sentiment, top gainers/losers
  - Options: historical options chain (premium only)
  - Enumerator: listing status for catalog indexing

Commodity data (WTI, Brent, natural gas, copper, etc.) is omitted — use the
FRED connector instead, which has superior historical coverage for those series.

Internal layout (not part of the public contract):

* :mod:`parsimony_alpha_vantage._http` — shared transport, unified error
  mapping (HTTP + Alpha Vantage's in-body error envelopes), URL redaction,
  JSON and CSV fetch helpers, key normalisers.
* :mod:`parsimony_alpha_vantage.params` — Pydantic parameter models.
* :mod:`parsimony_alpha_vantage.outputs` — declarative
  :class:`OutputConfig` schemas.

This ``__init__.py`` stays at the top level so ``tools/gen_registry.py``
can AST-parse ``@connector`` decorators (it does not follow re-exports).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Provenance, Result

from parsimony_alpha_vantage._http import av_fetch as _av_fetch
from parsimony_alpha_vantage._http import av_fetch_csv as _av_fetch_csv
from parsimony_alpha_vantage._http import clean_none_strings as _clean_none_strings
from parsimony_alpha_vantage._http import make_http as _make_http
from parsimony_alpha_vantage._http import strip_numbered_keys as _strip_numbered_keys
from parsimony_alpha_vantage.outputs import CRYPTO_DAILY_OUTPUT as _CRYPTO_DAILY_OUTPUT
from parsimony_alpha_vantage.outputs import DAILY_OUTPUT as _DAILY_OUTPUT
from parsimony_alpha_vantage.outputs import EARNINGS_CAL_OUTPUT as _EARNINGS_CAL_OUTPUT
from parsimony_alpha_vantage.outputs import EARNINGS_OUTPUT as _EARNINGS_OUTPUT
from parsimony_alpha_vantage.outputs import ECON_OUTPUT as _ECON_OUTPUT
from parsimony_alpha_vantage.outputs import FX_DAILY_OUTPUT as _FX_DAILY_OUTPUT
from parsimony_alpha_vantage.outputs import FX_RATE_OUTPUT as _FX_RATE_OUTPUT
from parsimony_alpha_vantage.outputs import INTRADAY_OUTPUT as _INTRADAY_OUTPUT
from parsimony_alpha_vantage.outputs import IPO_CAL_OUTPUT as _IPO_CAL_OUTPUT
from parsimony_alpha_vantage.outputs import LISTING_OUTPUT as _LISTING_OUTPUT
from parsimony_alpha_vantage.outputs import METAL_HISTORY_OUTPUT as _METAL_HISTORY_OUTPUT
from parsimony_alpha_vantage.outputs import METAL_SPOT_OUTPUT as _METAL_SPOT_OUTPUT
from parsimony_alpha_vantage.outputs import MOVERS_OUTPUT as _MOVERS_OUTPUT
from parsimony_alpha_vantage.outputs import NEWS_OUTPUT as _NEWS_OUTPUT
from parsimony_alpha_vantage.outputs import OPTIONS_OUTPUT as _OPTIONS_OUTPUT
from parsimony_alpha_vantage.outputs import QUOTE_OUTPUT as _QUOTE_OUTPUT
from parsimony_alpha_vantage.outputs import SEARCH_OUTPUT as _SEARCH_OUTPUT
from parsimony_alpha_vantage.outputs import TECHNICAL_OUTPUT as _TECHNICAL_OUTPUT
from parsimony_alpha_vantage.params import (
    AlphaVantageCryptoDailyParams,
    AlphaVantageCryptoMonthlyParams,
    AlphaVantageCryptoWeeklyParams,
    AlphaVantageDailyParams,
    AlphaVantageEarningsCalendarParams,
    AlphaVantageEarningsParams,
    AlphaVantageEconParams,
    AlphaVantageEtfProfileParams,
    AlphaVantageFxDailyParams,
    AlphaVantageFxMonthlyParams,
    AlphaVantageFxRateParams,
    AlphaVantageFxWeeklyParams,
    AlphaVantageIntradayParams,
    AlphaVantageIpoCalendarParams,
    AlphaVantageListingParams,
    AlphaVantageMetalHistoryParams,
    AlphaVantageMetalSpotParams,
    AlphaVantageMonthlyParams,
    AlphaVantageNewsParams,
    AlphaVantageOptionsParams,
    AlphaVantageOverviewParams,
    AlphaVantageQuoteParams,
    AlphaVantageSearchParams,
    AlphaVantageStatementParams,
    AlphaVantageTechnicalParams,
    AlphaVantageTopMoversParams,
    AlphaVantageWeeklyParams,
)

ENV_VARS: dict[str, str] = {"api_key": "ALPHA_VANTAGE_API_KEY"}

_PROVIDER = "alpha_vantage"


# ---------------------------------------------------------------------------
# Discovery — Symbol Search
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["equities", "tool"])
async def alpha_vantage_search(params: AlphaVantageSearchParams, *, api_key: str) -> Result:
    """Search Alpha Vantage for stocks, ETFs, and mutual funds by name or ticker.

    Returns symbol (the ticker), name, type (Equity/ETF), region, and currency.
    Use symbol with alpha_vantage_quote, alpha_vantage_daily, or
    alpha_vantage_overview for further data.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="SYMBOL_SEARCH",
        params={"keywords": params.keywords},
        op_name="alpha_vantage_search",
    )

    matches = data.get("bestMatches", [])
    if not matches:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No symbols found for: {params.keywords}",
        )

    rows = []
    for m in matches:
        s = _strip_numbered_keys(m)
        if not s.get("symbol"):
            continue
        rows.append(
            {
                "symbol": s.get("symbol", ""),
                "name": s.get("name", ""),
                "type": s.get("type", ""),
                "region": s.get("region", ""),
                "currency": s.get("currency", ""),
                "matchScore": s.get("matchScore", ""),
            }
        )
    df = pd.DataFrame(rows)
    return _SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_search", params={"keywords": params.keywords}),
        params={"keywords": params.keywords},
    )


# ---------------------------------------------------------------------------
# Market Data — Real-time Quote
# ---------------------------------------------------------------------------


@connector(output=_QUOTE_OUTPUT, tags=["equities"])
async def alpha_vantage_quote(params: AlphaVantageQuoteParams, *, api_key: str) -> Result:
    """Fetch real-time quote for a stock: current price, day high/low/open,
    volume, previous close, and change/change percent.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="GLOBAL_QUOTE",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_quote",
    )

    quote = data.get("Global Quote", {})
    if not quote:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No quote data returned for symbol: {params.symbol}",
        )

    q = _strip_numbered_keys(quote)
    change_pct_raw = q.get("change percent", "0")
    change_pct = change_pct_raw.rstrip("%") if isinstance(change_pct_raw, str) else change_pct_raw

    row = {
        "symbol": q.get("symbol", params.symbol),
        "price": q.get("price"),
        "open": q.get("open"),
        "high": q.get("high"),
        "low": q.get("low"),
        "volume": q.get("volume"),
        "latest_trading_day": q.get("latest trading day"),
        "previous_close": q.get("previous close"),
        "change": q.get("change"),
        "change_percent": change_pct,
    }
    df = pd.DataFrame([row])
    return _QUOTE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_quote", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Market Data — Daily Time Series
# ---------------------------------------------------------------------------


@connector(output=_DAILY_OUTPUT, tags=["equities"])
async def alpha_vantage_daily(params: AlphaVantageDailyParams, *, api_key: str) -> Result:
    """Fetch daily OHLCV (open, high, low, close, volume) time series for a stock.

    outputsize='compact' returns the last 100 trading days (default).
    outputsize='full' returns 20+ years of daily history.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_DAILY",
        params={"symbol": params.symbol, "outputsize": params.outputsize},
        op_name="alpha_vantage_daily",
    )

    ts_key = "Time Series (Daily)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No daily data returned for symbol: {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)

    meta = data.get("Meta Data", {})
    meta_clean = _strip_numbered_keys(meta)
    metadata_list = [{"name": k, "value": str(v)} for k, v in meta_clean.items()]

    prov = Provenance(
        source="alpha_vantage_daily",
        params={"symbol": params.symbol, "outputsize": params.outputsize},
        properties={"metadata": metadata_list},
    )
    return _DAILY_OUTPUT.build_table_result(
        df,
        provenance=prov,
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Company — Overview
# ---------------------------------------------------------------------------


@connector(tags=["equities"])
async def alpha_vantage_overview(params: AlphaVantageOverviewParams, *, api_key: str) -> Result:
    """Fetch company fundamentals for a stock: name, exchange, sector, industry,
    market cap, PE ratio, EPS, dividend yield, 52-week high/low, beta, and ~50
    more financial metrics. Returns a flat dict of string values.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="OVERVIEW",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_overview",
    )

    if not isinstance(data, dict) or not data.get("Symbol"):
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No overview data returned for symbol: {params.symbol}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="alpha_vantage_overview", params={"symbol": params.symbol}),
    )


# ---------------------------------------------------------------------------
# Company — Financial Statements (income, balance sheet, cash flow)
# ---------------------------------------------------------------------------


@connector(tags=["equities"])
async def alpha_vantage_income_statement(params: AlphaVantageStatementParams, *, api_key: str) -> Result:
    """Fetch income statement for a stock: revenue, gross profit, operating income,
    EBITDA, net income, R&D, SGA, and ~20 more line items. Returns annual or
    quarterly reports (up to 20 annual, 81 quarterly). All values are strings.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="INCOME_STATEMENT",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_income_statement",
    )

    key = "annualReports" if params.period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No income statement data for {params.symbol} ({params.period})",
        )

    return Result(
        data=reports,
        provenance=Provenance(
            source="alpha_vantage_income_statement",
            params={"symbol": params.symbol, "period": params.period},
        ),
    )


@connector(tags=["equities"])
async def alpha_vantage_balance_sheet(params: AlphaVantageStatementParams, *, api_key: str) -> Result:
    """Fetch balance sheet for a stock: total assets, liabilities, equity,
    cash, receivables, goodwill, long-term debt, and ~35 more line items.
    Returns annual or quarterly reports. All values are strings.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="BALANCE_SHEET",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_balance_sheet",
    )

    key = "annualReports" if params.period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No balance sheet data for {params.symbol} ({params.period})",
        )

    return Result(
        data=reports,
        provenance=Provenance(
            source="alpha_vantage_balance_sheet",
            params={"symbol": params.symbol, "period": params.period},
        ),
    )


@connector(tags=["equities"])
async def alpha_vantage_cash_flow(params: AlphaVantageStatementParams, *, api_key: str) -> Result:
    """Fetch cash flow statement for a stock: operating cash flow, capex,
    dividends, buybacks, financing, investing activities, and ~25 more items.
    Returns annual or quarterly reports. All values are strings.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="CASH_FLOW",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_cash_flow",
    )

    key = "annualReports" if params.period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No cash flow data for {params.symbol} ({params.period})",
        )

    return Result(
        data=reports,
        provenance=Provenance(
            source="alpha_vantage_cash_flow",
            params={"symbol": params.symbol, "period": params.period},
        ),
    )


# ---------------------------------------------------------------------------
# Company — Earnings
# ---------------------------------------------------------------------------


@connector(output=_EARNINGS_OUTPUT, tags=["equities"])
async def alpha_vantage_earnings(params: AlphaVantageEarningsParams, *, api_key: str) -> Result:
    """Fetch quarterly earnings for a stock: reported EPS, estimated EPS,
    surprise, surprise percentage, and report timing (pre/post market).
    Returns up to 120 quarters of history.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="EARNINGS",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_earnings",
    )

    # Use quarterly earnings for richer data
    reports = data.get("quarterlyEarnings", [])
    if not reports:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No earnings data for {params.symbol}",
        )

    rows = [_clean_none_strings(r) for r in reports if r.get("fiscalDateEnding")]
    df = pd.DataFrame(rows)
    return _EARNINGS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_earnings", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Forex — Real-time Exchange Rate
# ---------------------------------------------------------------------------


@connector(output=_FX_RATE_OUTPUT, tags=["forex", "crypto", "tool"])
async def alpha_vantage_fx_rate(params: AlphaVantageFxRateParams, *, api_key: str) -> Result:
    """Fetch real-time exchange rate between two currencies. Works for both
    forex (EUR/USD) and crypto (BTC/USD). Returns bid/ask prices.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="CURRENCY_EXCHANGE_RATE",
        params={
            "from_currency": params.from_currency,
            "to_currency": params.to_currency,
        },
        op_name="alpha_vantage_fx_rate",
    )

    rate_data = data.get("Realtime Currency Exchange Rate", {})
    if not rate_data:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No exchange rate for {params.from_currency}/{params.to_currency}",
        )

    r = _strip_numbered_keys(rate_data)
    row = {
        "from_currency": r.get("From_Currency Code", params.from_currency),
        "from_currency_name": r.get("From_Currency Name", ""),
        "to_currency": r.get("To_Currency Code", params.to_currency),
        "to_currency_name": r.get("To_Currency Name", ""),
        "exchange_rate": r.get("Exchange Rate"),
        "bid_price": r.get("Bid Price"),
        "ask_price": r.get("Ask Price"),
        "last_refreshed": r.get("Last Refreshed", ""),
    }
    df = pd.DataFrame([row])
    return _FX_RATE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_rate",
            params={
                "from_currency": params.from_currency,
                "to_currency": params.to_currency,
            },
        ),
        params={"from_currency": params.from_currency},
    )


# ---------------------------------------------------------------------------
# Forex — Historical Daily
# ---------------------------------------------------------------------------


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"])
async def alpha_vantage_fx_daily(params: AlphaVantageFxDailyParams, *, api_key: str) -> Result:
    """Fetch daily forex OHLC time series for a currency pair.

    outputsize='compact' returns last 100 days (default); 'full' for full history.
    Note: no volume data for forex pairs.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="FX_DAILY",
        params={
            "from_symbol": params.from_symbol,
            "to_symbol": params.to_symbol,
            "outputsize": params.outputsize,
        },
        op_name="alpha_vantage_fx_daily",
    )

    ts_key = "Time Series FX (Daily)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No FX daily data for {params.from_symbol}/{params.to_symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
            }
        )

    pair = f"{params.from_symbol}/{params.to_symbol}"
    df = pd.DataFrame(rows)
    return _FX_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_daily",
            params={
                "from_symbol": params.from_symbol,
                "to_symbol": params.to_symbol,
            },
        ),
        params={"pair": pair},
    )


# ---------------------------------------------------------------------------
# Crypto — Historical Daily
# ---------------------------------------------------------------------------


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"])
async def alpha_vantage_crypto_daily(params: AlphaVantageCryptoDailyParams, *, api_key: str) -> Result:
    """Fetch daily OHLCV time series for a cryptocurrency priced in a market
    currency (default USD). Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="DIGITAL_CURRENCY_DAILY",
        params={"symbol": params.symbol, "market": params.market},
        op_name="alpha_vantage_crypto_daily",
    )

    ts_key = "Time Series (Digital Currency Daily)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No crypto daily data for {params.symbol}/{params.market}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_crypto_daily",
            params={"symbol": params.symbol, "market": params.market},
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Economic Indicators
# ---------------------------------------------------------------------------


@connector(output=_ECON_OUTPUT, tags=["macro"])
async def alpha_vantage_econ(params: AlphaVantageEconParams, *, api_key: str) -> Result:
    """Fetch US economic indicator time series. Covers real GDP, CPI, inflation,
    unemployment, federal funds rate, treasury yield (with maturity selection),
    retail sales, durables, and nonfarm payroll. Commodity data is available
    via the FRED connector instead (superior historical coverage).
    Use maturity param for TREASURY_YIELD (default 10year).
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {}
    if params.interval is not None:
        req_params["interval"] = params.interval
    if params.maturity is not None and params.function == "TREASURY_YIELD":
        req_params["maturity"] = params.maturity

    data = await _av_fetch(
        http,
        function=params.function,
        params=req_params or None,
        op_name="alpha_vantage_econ",
    )

    observations = data.get("data", [])
    if not observations:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No data for {params.function}",
        )

    series_name = data.get("name", params.function)
    unit = data.get("unit", "")
    interval = data.get("interval", "")

    rows = []
    for obs in observations:
        val = obs.get("value")
        if val == ".":
            val = None
        rows.append(
            {
                "name": params.function,
                "series_name": series_name,
                "date": obs.get("date"),
                "value": val,
                "unit": unit,
                "interval": interval,
            }
        )

    df = pd.DataFrame(rows)
    prov_params: dict[str, Any] = {"function": params.function}
    if params.interval:
        prov_params["interval"] = params.interval
    if params.maturity:
        prov_params["maturity"] = params.maturity

    return _ECON_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_econ", params=prov_params),
        params={"name": params.function},
    )


# ---------------------------------------------------------------------------
# Alpha Intelligence — News Sentiment
# ---------------------------------------------------------------------------


@connector(output=_NEWS_OUTPUT, tags=["news", "tool"])
async def alpha_vantage_news(params: AlphaVantageNewsParams, *, api_key: str) -> Result:
    """Fetch news articles with sentiment scores. Filter by ticker(s) and/or
    topics. Each article includes title, summary, source, sentiment score
    (-1 to 1), and sentiment label. For ticker-specific sentiment, check the
    ticker_sentiment array in the raw response.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {"sort": params.sort, "limit": params.limit}
    if params.tickers:
        req_params["tickers"] = params.tickers
    if params.topics:
        req_params["topics"] = params.topics

    data = await _av_fetch(
        http,
        function="NEWS_SENTIMENT",
        params=req_params,
        op_name="alpha_vantage_news",
    )

    feed = data.get("feed", [])
    if not feed:
        raise EmptyDataError(
            provider=_PROVIDER,
            message="No news articles found",
        )

    rows = [
        {
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "time_published": item.get("time_published", ""),
            "source": item.get("source", ""),
            "overall_sentiment_score": item.get("overall_sentiment_score"),
            "overall_sentiment_label": item.get("overall_sentiment_label", ""),
            "summary": item.get("summary", ""),
            "banner_image": item.get("banner_image", ""),
        }
        for item in feed
    ]
    df = pd.DataFrame(rows)
    prov_params: dict[str, Any] = {}
    if params.tickers:
        prov_params["tickers"] = params.tickers
    if params.topics:
        prov_params["topics"] = params.topics

    return _NEWS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_news", params=prov_params),
        params=prov_params,
    )


# ---------------------------------------------------------------------------
# Alpha Intelligence — Top Gainers/Losers
# ---------------------------------------------------------------------------


@connector(output=_MOVERS_OUTPUT, tags=["equities", "tool"])
async def alpha_vantage_top_movers(params: AlphaVantageTopMoversParams, *, api_key: str) -> Result:
    """Fetch today's top 20 gainers, top 20 losers, and top 20 most actively
    traded US equities. Each entry includes ticker, price, change amount,
    change percentage, and volume. No parameters required.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TOP_GAINERS_LOSERS",
        op_name="alpha_vantage_top_movers",
    )

    rows = []
    for category in ("top_gainers", "top_losers", "most_actively_traded"):
        items = data.get(category, [])
        for item in items:
            pct_raw = item.get("change_percentage", "0")
            pct = pct_raw.rstrip("%") if isinstance(pct_raw, str) else pct_raw
            rows.append(
                {
                    "ticker": item.get("ticker", ""),
                    "category": category,
                    "price": item.get("price"),
                    "change_amount": item.get("change_amount"),
                    "change_percentage": pct,
                    "volume": item.get("volume"),
                }
            )

    if not rows:
        raise EmptyDataError(provider=_PROVIDER, message="No market movers data returned")

    df = pd.DataFrame(rows)
    return _MOVERS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_top_movers", params={}),
        params={},
    )


# ---------------------------------------------------------------------------
# Options — Historical Options Chain (premium only)
# ---------------------------------------------------------------------------


@connector(output=_OPTIONS_OUTPUT, tags=["equities", "options"])
async def alpha_vantage_options(params: AlphaVantageOptionsParams, *, api_key: str) -> Result:
    """[Premium] Fetch historical options chain for a stock: contract ID,
    expiration, strike, type (call/put), last price, bid/ask, volume,
    open interest, implied volatility, and Greeks (delta, gamma, theta, vega).
    Requires a premium Alpha Vantage plan.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {"symbol": params.symbol}
    if params.date:
        req_params["date"] = params.date

    data = await _av_fetch(
        http,
        function="HISTORICAL_OPTIONS",
        params=req_params,
        op_name="alpha_vantage_options",
    )

    # Response is a list of option contracts
    contracts = data if isinstance(data, list) else data.get("data", [])
    if not contracts:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No options data for {params.symbol}",
        )

    df = pd.DataFrame(contracts)
    prov_params: dict[str, Any] = {"symbol": params.symbol}
    if params.date:
        prov_params["date"] = params.date

    return _OPTIONS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_options", params=prov_params),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Market Data — Weekly & Monthly Time Series
# ---------------------------------------------------------------------------


@connector(output=_DAILY_OUTPUT, tags=["equities"])
async def alpha_vantage_weekly(params: AlphaVantageWeeklyParams, *, api_key: str) -> Result:
    """Fetch weekly OHLCV (open, high, low, close, volume) time series for a stock.

    Returns 20+ years of weekly data. Last trading day of each week is the timestamp.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_WEEKLY",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_weekly",
    )

    ts_key = "Weekly Time Series"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No weekly data returned for symbol: {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_weekly", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


@connector(output=_DAILY_OUTPUT, tags=["equities"])
async def alpha_vantage_monthly(params: AlphaVantageMonthlyParams, *, api_key: str) -> Result:
    """Fetch monthly OHLCV (open, high, low, close, volume) time series for a stock.

    Returns 20+ years of monthly data. Last trading day of each month is the timestamp.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_MONTHLY",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_monthly",
    )

    ts_key = "Monthly Time Series"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No monthly data returned for symbol: {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_monthly", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Market Data — Intraday Time Series
# ---------------------------------------------------------------------------


@connector(output=_INTRADAY_OUTPUT, tags=["equities"])
async def alpha_vantage_intraday(params: AlphaVantageIntradayParams, *, api_key: str) -> Result:
    """Fetch intraday OHLCV time series for a stock at 1/5/15/30/60 min intervals.

    outputsize='compact' returns the last 100 data points (default).
    outputsize='full' returns the full intraday time series for the current and
    previous trading day.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_INTRADAY",
        params={
            "symbol": params.symbol,
            "interval": params.interval,
            "outputsize": params.outputsize,
        },
        op_name="alpha_vantage_intraday",
    )

    ts_key = f"Time Series ({params.interval})"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No intraday data for {params.symbol} at {params.interval}",
        )

    rows = []
    for ts_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "timestamp": ts_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _INTRADAY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_intraday",
            params={
                "symbol": params.symbol,
                "interval": params.interval,
                "outputsize": params.outputsize,
            },
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Forex — Weekly & Monthly
# ---------------------------------------------------------------------------


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"])
async def alpha_vantage_fx_weekly(params: AlphaVantageFxWeeklyParams, *, api_key: str) -> Result:
    """Fetch weekly forex OHLC time series for a currency pair.

    Returns full history of weekly data. No volume data for forex.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="FX_WEEKLY",
        params={
            "from_symbol": params.from_symbol,
            "to_symbol": params.to_symbol,
        },
        op_name="alpha_vantage_fx_weekly",
    )

    ts_key = "Time Series FX (Weekly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No FX weekly data for {params.from_symbol}/{params.to_symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
            }
        )

    pair = f"{params.from_symbol}/{params.to_symbol}"
    df = pd.DataFrame(rows)
    return _FX_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_weekly",
            params={
                "from_symbol": params.from_symbol,
                "to_symbol": params.to_symbol,
            },
        ),
        params={"pair": pair},
    )


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"])
async def alpha_vantage_fx_monthly(params: AlphaVantageFxMonthlyParams, *, api_key: str) -> Result:
    """Fetch monthly forex OHLC time series for a currency pair.

    Returns full history of monthly data. No volume data for forex.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="FX_MONTHLY",
        params={
            "from_symbol": params.from_symbol,
            "to_symbol": params.to_symbol,
        },
        op_name="alpha_vantage_fx_monthly",
    )

    ts_key = "Time Series FX (Monthly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No FX monthly data for {params.from_symbol}/{params.to_symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
            }
        )

    pair = f"{params.from_symbol}/{params.to_symbol}"
    df = pd.DataFrame(rows)
    return _FX_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_fx_monthly",
            params={
                "from_symbol": params.from_symbol,
                "to_symbol": params.to_symbol,
            },
        ),
        params={"pair": pair},
    )


# ---------------------------------------------------------------------------
# Crypto — Weekly & Monthly
# ---------------------------------------------------------------------------


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"])
async def alpha_vantage_crypto_weekly(params: AlphaVantageCryptoWeeklyParams, *, api_key: str) -> Result:
    """Fetch weekly OHLCV time series for a cryptocurrency. Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="DIGITAL_CURRENCY_WEEKLY",
        params={"symbol": params.symbol, "market": params.market},
        op_name="alpha_vantage_crypto_weekly",
    )

    ts_key = "Time Series (Digital Currency Weekly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No crypto weekly data for {params.symbol}/{params.market}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_crypto_weekly",
            params={"symbol": params.symbol, "market": params.market},
        ),
        params={"symbol": params.symbol},
    )


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"])
async def alpha_vantage_crypto_monthly(params: AlphaVantageCryptoMonthlyParams, *, api_key: str) -> Result:
    """Fetch monthly OHLCV time series for a cryptocurrency. Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="DIGITAL_CURRENCY_MONTHLY",
        params={"symbol": params.symbol, "market": params.market},
        op_name="alpha_vantage_crypto_monthly",
    )

    ts_key = "Time Series (Digital Currency Monthly)"
    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No crypto monthly data for {params.symbol}/{params.market}",
        )

    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                "date": date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    df = pd.DataFrame(rows)
    return _CRYPTO_DAILY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_crypto_monthly",
            params={"symbol": params.symbol, "market": params.market},
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Company — ETF Profile
# ---------------------------------------------------------------------------


@connector(tags=["equities", "etf"])
async def alpha_vantage_etf_profile(params: AlphaVantageEtfProfileParams, *, api_key: str) -> Result:
    """Fetch ETF profile: net assets, expense ratio, portfolio turnover,
    dividend yield, inception date, top holdings (symbol, description, weight),
    and sector allocation. Note: aggressive rate limiting on free tier.
    Use alpha_vantage_search to resolve ETF symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="ETF_PROFILE",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_etf_profile",
    )

    if not isinstance(data, dict) or (not data.get("holdings") and not data.get("net_assets")):
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No ETF profile data for {params.symbol}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="alpha_vantage_etf_profile", params={"symbol": params.symbol}),
    )


# ---------------------------------------------------------------------------
# Calendars — Earnings Calendar & IPO Calendar (CSV endpoints)
# ---------------------------------------------------------------------------


@connector(output=_EARNINGS_CAL_OUTPUT, tags=["equities", "calendars"])
async def alpha_vantage_earnings_calendar(params: AlphaVantageEarningsCalendarParams, *, api_key: str) -> Result:
    """Fetch upcoming earnings release dates. Returns company name, report date,
    fiscal date ending, EPS estimate, and currency. Filter by symbol or get
    all upcoming earnings within the horizon window.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    req_params: dict[str, Any] = {"horizon": params.horizon}
    if params.symbol:
        req_params["symbol"] = params.symbol

    df = await _av_fetch_csv(
        http,
        function="EARNINGS_CALENDAR",
        params=req_params,
        op_name="alpha_vantage_earnings_calendar",
    )

    if df.empty:
        raise EmptyDataError(
            provider=_PROVIDER,
            message="No upcoming earnings events found",
        )

    prov_params: dict[str, Any] = {"horizon": params.horizon}
    if params.symbol:
        prov_params["symbol"] = params.symbol

    return _EARNINGS_CAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_earnings_calendar", params=prov_params),
        params=prov_params,
    )


@connector(output=_IPO_CAL_OUTPUT, tags=["equities", "calendars"])
async def alpha_vantage_ipo_calendar(params: AlphaVantageIpoCalendarParams, *, api_key: str) -> Result:
    """Fetch upcoming and recent IPOs: company name, expected IPO date,
    price range (low/high), currency, and exchange.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    df = await _av_fetch_csv(
        http,
        function="IPO_CALENDAR",
        op_name="alpha_vantage_ipo_calendar",
    )

    if df.empty:
        raise EmptyDataError(
            provider=_PROVIDER,
            message="No upcoming IPO events found",
        )

    return _IPO_CAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_ipo_calendar", params={}),
        params={},
    )


# ---------------------------------------------------------------------------
# Technical Indicators (unified)
# ---------------------------------------------------------------------------


@connector(output=_TECHNICAL_OUTPUT, tags=["equities", "technical"])
async def alpha_vantage_technical(params: AlphaVantageTechnicalParams, *, api_key: str) -> Result:
    """Fetch a technical indicator for a stock. Supports 50+ indicators including
    SMA, EMA, RSI, MACD, Bollinger Bands, Stochastic, ADX, CCI, OBV, ATR, and more.
    The response columns vary by indicator (e.g. SMA returns 'SMA', BBANDS returns
    'Real Upper Band', 'Real Middle Band', 'Real Lower Band'). All values are numeric.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function=params.function,
        params={
            "symbol": params.symbol,
            "interval": params.interval,
            "time_period": params.time_period,
            "series_type": params.series_type,
        },
        op_name="alpha_vantage_technical",
    )

    # Technical Analysis key varies: "Technical Analysis: SMA", "Technical Analysis: RSI", etc.
    ta_key = f"Technical Analysis: {params.function}"
    time_series = data.get(ta_key, {})
    if not time_series:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No {params.function} data for {params.symbol}",
        )

    rows = []
    for date_str, values in time_series.items():
        row: dict[str, Any] = {"date": date_str}
        row.update(values)
        rows.append(row)

    df = pd.DataFrame(rows)
    # Convert all indicator value columns to numeric
    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return _TECHNICAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_technical",
            params={
                "symbol": params.symbol,
                "function": params.function,
                "interval": params.interval,
                "time_period": params.time_period,
                "series_type": params.series_type,
            },
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Precious Metals — Spot Price & Historical
# ---------------------------------------------------------------------------


@connector(output=_METAL_SPOT_OUTPUT, tags=["commodities"])
async def alpha_vantage_metal_spot(params: AlphaVantageMetalSpotParams, *, api_key: str) -> Result:
    """Fetch real-time spot price for gold or silver. Returns current price and
    timestamp. Use GOLD/XAU for gold, SILVER/XAG for silver.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="GOLD_SILVER_SPOT",
        params={"symbol": params.symbol},
        op_name="alpha_vantage_metal_spot",
    )

    if not isinstance(data, dict) or "price" not in data:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No spot price data for {params.symbol}",
        )

    row = {
        "symbol": params.symbol,
        "nominal": data.get("nominal", params.symbol),
        "price": data.get("price"),
        "timestamp": data.get("timestamp", ""),
    }
    df = pd.DataFrame([row])
    return _METAL_SPOT_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="alpha_vantage_metal_spot", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


@connector(output=_METAL_HISTORY_OUTPUT, tags=["commodities"])
async def alpha_vantage_metal_history(params: AlphaVantageMetalHistoryParams, *, api_key: str) -> Result:
    """Fetch historical prices for gold or silver. Returns date and price.
    Note: uses 'price' field (not 'value') unlike other commodity endpoints.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _make_http(api_key)
    data = await _av_fetch(
        http,
        function="GOLD_SILVER_HISTORY",
        params={"symbol": params.symbol, "interval": params.interval},
        op_name="alpha_vantage_metal_history",
    )

    observations = data.get("data", [])
    if not observations:
        raise EmptyDataError(
            provider=_PROVIDER,
            message=f"No historical data for {params.symbol}",
        )

    rows = []
    for obs in observations:
        price = obs.get("price")
        if price == ".":
            price = None
        rows.append({"date": obs.get("date"), "price": price})

    df = pd.DataFrame(rows)
    return _METAL_HISTORY_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="alpha_vantage_metal_history",
            params={"symbol": params.symbol, "interval": params.interval},
        ),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Listing Status — Enumerator
# ---------------------------------------------------------------------------


@enumerator(output=_LISTING_OUTPUT, tags=["equities"])
async def enumerate_alpha_vantage(params: AlphaVantageListingParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate all US-listed securities from Alpha Vantage for catalog indexing.

    Returns symbol, name, exchange, asset type (Stock/ETF), IPO date, and status.
    Use state='active' for current listings (default), 'delisted' for historical.
    """
    http = _make_http(api_key)
    df = await _av_fetch_csv(
        http,
        function="LISTING_STATUS",
        params={"state": params.state},
        op_name="enumerate_alpha_vantage",
    )

    if df.empty:
        return pd.DataFrame(columns=["symbol", "name", "exchange", "assetType", "ipoDate", "status"])

    # Keep only the columns we care about
    keep = ["symbol", "name", "exchange", "assetType", "ipoDate", "status"]
    cols = [c for c in keep if c in df.columns]
    return df[cols]


# ---------------------------------------------------------------------------
# Collection — kept as a literal ``Connectors([...])`` assignment so
# ``tools/gen_registry.py`` can AST-extract it.
# ---------------------------------------------------------------------------

CONNECTORS = Connectors(
    [
        # Discovery
        alpha_vantage_search,
        # Market data
        alpha_vantage_quote,
        alpha_vantage_daily,
        alpha_vantage_weekly,
        alpha_vantage_monthly,
        alpha_vantage_intraday,
        # Company fundamentals
        alpha_vantage_overview,
        alpha_vantage_income_statement,
        alpha_vantage_balance_sheet,
        alpha_vantage_cash_flow,
        alpha_vantage_earnings,
        alpha_vantage_etf_profile,
        # Calendars
        alpha_vantage_earnings_calendar,
        alpha_vantage_ipo_calendar,
        # Forex
        alpha_vantage_fx_rate,
        alpha_vantage_fx_daily,
        alpha_vantage_fx_weekly,
        alpha_vantage_fx_monthly,
        # Crypto
        alpha_vantage_crypto_daily,
        alpha_vantage_crypto_weekly,
        alpha_vantage_crypto_monthly,
        # Economic indicators
        alpha_vantage_econ,
        # Precious metals (real-time spot — not available via FRED)
        alpha_vantage_metal_spot,
        alpha_vantage_metal_history,
        # Alpha intelligence
        alpha_vantage_news,
        alpha_vantage_top_movers,
        # Technical indicators
        alpha_vantage_technical,
        # Options
        alpha_vantage_options,
        # Enumeration
        enumerate_alpha_vantage,
    ]
)


__all__ = [
    "CONNECTORS",
    "ENV_VARS",
    # Parameter models (public — downstream callers type against these)
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
    # Connector functions
    "alpha_vantage_balance_sheet",
    "alpha_vantage_cash_flow",
    "alpha_vantage_crypto_daily",
    "alpha_vantage_crypto_monthly",
    "alpha_vantage_crypto_weekly",
    "alpha_vantage_daily",
    "alpha_vantage_earnings",
    "alpha_vantage_earnings_calendar",
    "alpha_vantage_econ",
    "alpha_vantage_etf_profile",
    "alpha_vantage_fx_daily",
    "alpha_vantage_fx_monthly",
    "alpha_vantage_fx_rate",
    "alpha_vantage_fx_weekly",
    "alpha_vantage_income_statement",
    "alpha_vantage_intraday",
    "alpha_vantage_ipo_calendar",
    "alpha_vantage_metal_history",
    "alpha_vantage_metal_spot",
    "alpha_vantage_monthly",
    "alpha_vantage_news",
    "alpha_vantage_options",
    "alpha_vantage_overview",
    "alpha_vantage_quote",
    "alpha_vantage_search",
    "alpha_vantage_top_movers",
    "alpha_vantage_weekly",
    # Enumerator
    "enumerate_alpha_vantage",
]
