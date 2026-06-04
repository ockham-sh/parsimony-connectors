"""Alpha Vantage connectors — 28 ``@connector`` verbs + 1 ``@enumerator``.

Every verb resolves its client through the shared ``_client`` (§4.3 keyed
template: arg → ``ALPHA_VANTAGE_API_KEY`` env fallback → fast-fail), declares
``secrets=("api_key",)`` so the key is stripped from provenance, routes through
``av_fetch`` / ``av_fetch_csv`` (which run the §5.8 in-body error check), and
validates call-time arguments inline (raising ``InvalidParameterError``).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

import pandas as pd
from parsimony.connector import connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError

from parsimony_alpha_vantage._http import _client
from parsimony_alpha_vantage._http import av_fetch as _av_fetch
from parsimony_alpha_vantage._http import av_fetch_csv as _av_fetch_csv
from parsimony_alpha_vantage._http import clean_none_strings as _clean_none_strings
from parsimony_alpha_vantage._http import strip_numbered_keys as _strip_numbered_keys
from parsimony_alpha_vantage.outputs import CRYPTO_DAILY_OUTPUT as _CRYPTO_DAILY_OUTPUT
from parsimony_alpha_vantage.outputs import DAILY_OUTPUT as _DAILY_OUTPUT
from parsimony_alpha_vantage.outputs import EARNINGS_CAL_OUTPUT as _EARNINGS_CAL_OUTPUT
from parsimony_alpha_vantage.outputs import EARNINGS_OUTPUT as _EARNINGS_OUTPUT
from parsimony_alpha_vantage.outputs import ECON_OUTPUT as _ECON_OUTPUT
from parsimony_alpha_vantage.outputs import ETF_PROFILE_OUTPUT as _ETF_PROFILE_OUTPUT
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
from parsimony_alpha_vantage.outputs import OVERVIEW_OUTPUT as _OVERVIEW_OUTPUT
from parsimony_alpha_vantage.outputs import QUOTE_OUTPUT as _QUOTE_OUTPUT
from parsimony_alpha_vantage.outputs import SEARCH_OUTPUT as _SEARCH_OUTPUT
from parsimony_alpha_vantage.outputs import STATEMENT_OUTPUT as _STATEMENT_OUTPUT
from parsimony_alpha_vantage.outputs import TECHNICAL_OUTPUT as _TECHNICAL_OUTPUT

_PROVIDER = "alpha_vantage"

# Economic-indicator function names accepted by alpha_vantage_econ.
_ECON_FUNCTIONS: tuple[str, ...] = (
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
_TECHNICAL_INDICATORS: tuple[str, ...] = (
    "SMA", "EMA", "WMA", "DEMA", "TEMA", "TRIMA", "KAMA", "MAMA", "VWAP", "T3",
    "RSI", "WILLR", "ADX", "ADXR", "APO", "PPO", "MOM", "BOP", "CCI", "CMO",
    "ROC", "ROCR", "AROON", "AROONOSC", "MFI", "TRIX", "ULTOSC", "DX",
    "MINUS_DI", "PLUS_DI", "MINUS_DM", "PLUS_DM", "BBANDS", "MIDPOINT",
    "MIDPRICE", "SAR", "TRANGE", "ATR", "NATR", "AD", "ADOSC", "OBV",
    "HT_TRENDLINE", "HT_SINE", "HT_TRENDMODE", "HT_DCPERIOD", "HT_DCPHASE",
    "HT_PHASOR", "STOCH", "STOCHF", "STOCHRSI", "MACD", "MACDEXT",
)


def _require_nonempty(value: str, name: str) -> str:
    """Validate a required scalar string argument before any network call."""
    cleaned = (value or "").strip()
    if not cleaned:
        raise InvalidParameterError(_PROVIDER, f"{name} must be a non-empty string")
    return cleaned


# ---------------------------------------------------------------------------
# Discovery — Symbol Search
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["equities", "tool"], secrets=("api_key",))
async def alpha_vantage_search(keywords: str, *, api_key: str = "") -> Any:
    """Search Alpha Vantage for stocks, ETFs, and mutual funds by name or ticker.

    Returns symbol (the ticker), name, type (Equity/ETF), region, and currency.
    Use symbol with alpha_vantage_quote, alpha_vantage_daily, or
    alpha_vantage_overview for further data.
    Free tier: 25 requests/day total across all endpoints.
    """
    keywords = _require_nonempty(keywords, "keywords")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="SYMBOL_SEARCH",
        params={"keywords": keywords},
        op_name="alpha_vantage_search",
    )

    matches = data.get("bestMatches", [])
    if not matches:
        raise EmptyDataError(_PROVIDER, query_params={"keywords": keywords})

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
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"keywords": keywords})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Market Data — Real-time Quote
# ---------------------------------------------------------------------------


@connector(output=_QUOTE_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_quote(symbol: Annotated[str, "ns:alpha_vantage"], *, api_key: str = "") -> Any:
    """Fetch real-time quote for a stock: current price, day high/low/open,
    volume, previous close, and change/change percent.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="GLOBAL_QUOTE",
        params={"symbol": symbol},
        op_name="alpha_vantage_quote",
    )

    quote = data.get("Global Quote", {})
    if not quote:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    q = _strip_numbered_keys(quote)
    change_pct_raw = q.get("change percent", "0")
    change_pct = change_pct_raw.rstrip("%") if isinstance(change_pct_raw, str) else change_pct_raw

    row = {
        "symbol": q.get("symbol", symbol),
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
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# Market Data — Daily / Weekly / Monthly Time Series
# ---------------------------------------------------------------------------


def _ohlcv_rows(time_series: dict[str, Any], *, date_field: str = "date") -> list[dict[str, Any]]:
    """Shape an Alpha Vantage OHLCV time-series dict into row dicts."""
    rows = []
    for date_str, values in time_series.items():
        v = _strip_numbered_keys(values)
        rows.append(
            {
                date_field: date_str,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )
    return rows


@connector(output=_DAILY_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_daily(
    symbol: Annotated[str, "ns:alpha_vantage"],
    outputsize: Literal["compact", "full"] = "compact",
    *,
    api_key: str = "",
) -> Any:
    """Fetch daily OHLCV (open, high, low, close, volume) time series for a stock.

    outputsize='compact' returns the last 100 trading days (default).
    outputsize='full' returns 20+ years of daily history.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_DAILY",
        params={"symbol": symbol, "outputsize": outputsize},
        op_name="alpha_vantage_daily",
    )

    time_series = data.get("Time Series (Daily)", {})
    if not time_series:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    df = pd.DataFrame(_ohlcv_rows(time_series))
    df["symbol"] = symbol
    # Project to declared columns only — the Meta Data block is provider chrome.
    return df[[c.name for c in _DAILY_OUTPUT.columns]]


@connector(output=_DAILY_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_weekly(symbol: Annotated[str, "ns:alpha_vantage"], *, api_key: str = "") -> Any:
    """Fetch weekly OHLCV (open, high, low, close, volume) time series for a stock.

    Returns 20+ years of weekly data. Last trading day of each week is the timestamp.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_WEEKLY",
        params={"symbol": symbol},
        op_name="alpha_vantage_weekly",
    )

    time_series = data.get("Weekly Time Series", {})
    if not time_series:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    df = pd.DataFrame(_ohlcv_rows(time_series))
    df["symbol"] = symbol
    return df[[c.name for c in _DAILY_OUTPUT.columns]]


@connector(output=_DAILY_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_monthly(symbol: Annotated[str, "ns:alpha_vantage"], *, api_key: str = "") -> Any:
    """Fetch monthly OHLCV (open, high, low, close, volume) time series for a stock.

    Returns 20+ years of monthly data. Last trading day of each month is the timestamp.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_MONTHLY",
        params={"symbol": symbol},
        op_name="alpha_vantage_monthly",
    )

    time_series = data.get("Monthly Time Series", {})
    if not time_series:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    df = pd.DataFrame(_ohlcv_rows(time_series))
    df["symbol"] = symbol
    return df[[c.name for c in _DAILY_OUTPUT.columns]]


@connector(output=_INTRADAY_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_intraday(
    symbol: Annotated[str, "ns:alpha_vantage"],
    interval: Literal["1min", "5min", "15min", "30min", "60min"] = "60min",
    outputsize: Literal["compact", "full"] = "compact",
    *,
    api_key: str = "",
) -> Any:
    """Fetch intraday OHLCV time series for a stock at 1/5/15/30/60 min intervals.

    outputsize='compact' returns the last 100 data points (default).
    outputsize='full' returns the full intraday time series for the current and
    previous trading day.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="TIME_SERIES_INTRADAY",
        params={"symbol": symbol, "interval": interval, "outputsize": outputsize},
        op_name="alpha_vantage_intraday",
    )

    time_series = data.get(f"Time Series ({interval})", {})
    if not time_series:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "interval": interval})

    df = pd.DataFrame(_ohlcv_rows(time_series, date_field="timestamp"))
    df["symbol"] = symbol
    return df[[c.name for c in _INTRADAY_OUTPUT.columns]]


# ---------------------------------------------------------------------------
# Company — Overview
# ---------------------------------------------------------------------------


@connector(output=_OVERVIEW_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_overview(symbol: Annotated[str, "ns:alpha_vantage"], *, api_key: str = "") -> Any:
    """Fetch company fundamentals for a stock: name, exchange, sector, industry,
    market cap, PE ratio, EPS, dividend yield, 52-week high/low, beta, and ~50
    more financial metrics. Returns a single keyed row.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="OVERVIEW",
        params={"symbol": symbol},
        op_name="alpha_vantage_overview",
    )

    if not isinstance(data, dict) or not data.get("Symbol"):
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    # Flat ~50-field dict → single keyed row; provider fields fold in as DATA.
    return pd.DataFrame([_clean_none_strings(data)])


# ---------------------------------------------------------------------------
# Company — Financial Statements (income, balance sheet, cash flow)
# ---------------------------------------------------------------------------


async def _statement(
    function: str,
    op_name: str,
    *,
    symbol: str,
    period: str,
    api_key: str,
) -> pd.DataFrame:
    """Shared income/balance/cash-flow loader → one keyed row per period."""
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(http, function=function, params={"symbol": symbol}, op_name=op_name)

    key = "annualReports" if period == "annual" else "quarterlyReports"
    reports = data.get(key, [])
    if not reports:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "period": period})

    rows = [_clean_none_strings(r) for r in reports if r.get("fiscalDateEnding")]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "period": period})
    df = pd.DataFrame(rows)
    df["symbol"] = symbol
    return df


@connector(output=_STATEMENT_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_income_statement(
    symbol: Annotated[str, "ns:alpha_vantage"],
    period: Literal["annual", "quarterly"] = "annual",
    *,
    api_key: str = "",
) -> Any:
    """Fetch income statement for a stock: revenue, gross profit, operating income,
    EBITDA, net income, R&D, SGA, and ~20 more line items. Returns one row per
    reporting period (annual or quarterly), keyed by symbol.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _statement(
        "INCOME_STATEMENT", "alpha_vantage_income_statement", symbol=symbol, period=period, api_key=api_key
    )


@connector(output=_STATEMENT_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_balance_sheet(
    symbol: Annotated[str, "ns:alpha_vantage"],
    period: Literal["annual", "quarterly"] = "annual",
    *,
    api_key: str = "",
) -> Any:
    """Fetch balance sheet for a stock: total assets, liabilities, equity,
    cash, receivables, goodwill, long-term debt, and ~35 more line items.
    Returns one row per reporting period (annual or quarterly), keyed by symbol.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _statement(
        "BALANCE_SHEET", "alpha_vantage_balance_sheet", symbol=symbol, period=period, api_key=api_key
    )


@connector(output=_STATEMENT_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_cash_flow(
    symbol: Annotated[str, "ns:alpha_vantage"],
    period: Literal["annual", "quarterly"] = "annual",
    *,
    api_key: str = "",
) -> Any:
    """Fetch cash flow statement for a stock: operating cash flow, capex,
    dividends, buybacks, financing, investing activities, and ~25 more items.
    Returns one row per reporting period (annual or quarterly), keyed by symbol.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _statement(
        "CASH_FLOW", "alpha_vantage_cash_flow", symbol=symbol, period=period, api_key=api_key
    )


# ---------------------------------------------------------------------------
# Company — Earnings
# ---------------------------------------------------------------------------


@connector(output=_EARNINGS_OUTPUT, tags=["equities"], secrets=("api_key",))
async def alpha_vantage_earnings(symbol: Annotated[str, "ns:alpha_vantage"], *, api_key: str = "") -> Any:
    """Fetch quarterly earnings for a stock: reported EPS, estimated EPS,
    surprise, surprise percentage, and report timing (pre/post market).
    Returns up to 120 quarters of history.
    Use alpha_vantage_search to resolve ticker symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="EARNINGS",
        params={"symbol": symbol},
        op_name="alpha_vantage_earnings",
    )

    reports = data.get("quarterlyEarnings", [])
    if not reports:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    rows = [_clean_none_strings(r) for r in reports if r.get("fiscalDateEnding")]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})
    df = pd.DataFrame(rows)
    df["symbol"] = symbol
    return df


# ---------------------------------------------------------------------------
# Company — ETF Profile
# ---------------------------------------------------------------------------


@connector(output=_ETF_PROFILE_OUTPUT, tags=["equities", "etf"], secrets=("api_key",))
async def alpha_vantage_etf_profile(symbol: Annotated[str, "ns:alpha_vantage"], *, api_key: str = "") -> Any:
    """Fetch ETF holdings: each row is a held security with its symbol,
    description, and portfolio weight. Note: aggressive rate limiting on free tier.
    Use alpha_vantage_search to resolve ETF symbols first.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="ETF_PROFILE",
        params={"symbol": symbol},
        op_name="alpha_vantage_etf_profile",
    )

    holdings = data.get("holdings", []) if isinstance(data, dict) else []
    if not holdings:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    rows = [
        {
            "symbol": symbol,
            "holding_symbol": h.get("symbol", ""),
            "description": h.get("description", ""),
            "weight": h.get("weight"),
        }
        for h in holdings
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Forex — Real-time Exchange Rate
# ---------------------------------------------------------------------------


@connector(output=_FX_RATE_OUTPUT, tags=["forex", "crypto", "tool"], secrets=("api_key",))
async def alpha_vantage_fx_rate(from_currency: str, to_currency: str, *, api_key: str = "") -> Any:
    """Fetch real-time exchange rate between two currencies. Works for both
    forex (EUR/USD) and crypto (BTC/USD). Returns bid/ask prices.
    Free tier: 25 requests/day total across all endpoints.
    """
    from_currency = _require_nonempty(from_currency, "from_currency")
    to_currency = _require_nonempty(to_currency, "to_currency")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="CURRENCY_EXCHANGE_RATE",
        params={"from_currency": from_currency, "to_currency": to_currency},
        op_name="alpha_vantage_fx_rate",
    )

    rate_data = data.get("Realtime Currency Exchange Rate", {})
    if not rate_data:
        raise EmptyDataError(
            _PROVIDER, query_params={"from_currency": from_currency, "to_currency": to_currency}
        )

    r = _strip_numbered_keys(rate_data)
    row = {
        "from_currency": r.get("From_Currency Code", from_currency),
        "from_currency_name": r.get("From_Currency Name", ""),
        "to_currency": r.get("To_Currency Code", to_currency),
        "to_currency_name": r.get("To_Currency Name", ""),
        "exchange_rate": r.get("Exchange Rate"),
        "bid_price": r.get("Bid Price"),
        "ask_price": r.get("Ask Price"),
        "last_refreshed": r.get("Last Refreshed", ""),
    }
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# Forex — Historical Daily / Weekly / Monthly
# ---------------------------------------------------------------------------


def _fx_rows(time_series: dict[str, Any]) -> list[dict[str, Any]]:
    """Shape an Alpha Vantage FX time-series dict into OHLC row dicts (no volume)."""
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
    return rows


async def _fx_series(
    function: str,
    ts_key: str,
    op_name: str,
    *,
    from_symbol: str,
    to_symbol: str,
    outputsize: str | None,
    api_key: str,
) -> pd.DataFrame:
    """Shared FX daily/weekly/monthly loader; injects the synthetic ``pair`` KEY."""
    from_symbol = _require_nonempty(from_symbol, "from_symbol")
    to_symbol = _require_nonempty(to_symbol, "to_symbol")
    http = _client(api_key)
    params: dict[str, Any] = {"from_symbol": from_symbol, "to_symbol": to_symbol}
    if outputsize is not None:
        params["outputsize"] = outputsize
    data = await _av_fetch(http, function=function, params=params, op_name=op_name)

    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(
            _PROVIDER, query_params={"from_symbol": from_symbol, "to_symbol": to_symbol}
        )

    df = pd.DataFrame(_fx_rows(time_series))
    df["pair"] = f"{from_symbol}/{to_symbol}"
    return df[[c.name for c in _FX_DAILY_OUTPUT.columns]]


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"], secrets=("api_key",))
async def alpha_vantage_fx_daily(
    from_symbol: str,
    to_symbol: str,
    outputsize: Literal["compact", "full"] = "compact",
    *,
    api_key: str = "",
) -> Any:
    """Fetch daily forex OHLC time series for a currency pair.

    outputsize='compact' returns last 100 days (default); 'full' for full history.
    Note: no volume data for forex pairs.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _fx_series(
        "FX_DAILY",
        "Time Series FX (Daily)",
        "alpha_vantage_fx_daily",
        from_symbol=from_symbol,
        to_symbol=to_symbol,
        outputsize=outputsize,
        api_key=api_key,
    )


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"], secrets=("api_key",))
async def alpha_vantage_fx_weekly(from_symbol: str, to_symbol: str, *, api_key: str = "") -> Any:
    """Fetch weekly forex OHLC time series for a currency pair.

    Returns full history of weekly data. No volume data for forex.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _fx_series(
        "FX_WEEKLY",
        "Time Series FX (Weekly)",
        "alpha_vantage_fx_weekly",
        from_symbol=from_symbol,
        to_symbol=to_symbol,
        outputsize=None,
        api_key=api_key,
    )


@connector(output=_FX_DAILY_OUTPUT, tags=["forex"], secrets=("api_key",))
async def alpha_vantage_fx_monthly(from_symbol: str, to_symbol: str, *, api_key: str = "") -> Any:
    """Fetch monthly forex OHLC time series for a currency pair.

    Returns full history of monthly data. No volume data for forex.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _fx_series(
        "FX_MONTHLY",
        "Time Series FX (Monthly)",
        "alpha_vantage_fx_monthly",
        from_symbol=from_symbol,
        to_symbol=to_symbol,
        outputsize=None,
        api_key=api_key,
    )


# ---------------------------------------------------------------------------
# Crypto — Historical Daily / Weekly / Monthly
# ---------------------------------------------------------------------------


async def _crypto_series(
    function: str,
    ts_key: str,
    op_name: str,
    *,
    symbol: str,
    market: str,
    api_key: str,
) -> pd.DataFrame:
    """Shared crypto daily/weekly/monthly loader; injects the ``symbol`` KEY."""
    symbol = _require_nonempty(symbol, "symbol")
    market = _require_nonempty(market, "market")
    http = _client(api_key)
    data = await _av_fetch(
        http, function=function, params={"symbol": symbol, "market": market}, op_name=op_name
    )

    time_series = data.get(ts_key, {})
    if not time_series:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "market": market})

    df = pd.DataFrame(_ohlcv_rows(time_series))
    df["symbol"] = symbol
    return df[[c.name for c in _CRYPTO_DAILY_OUTPUT.columns]]


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"], secrets=("api_key",))
async def alpha_vantage_crypto_daily(symbol: str, market: str = "USD", *, api_key: str = "") -> Any:
    """Fetch daily OHLCV time series for a cryptocurrency priced in a market
    currency (default USD). Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _crypto_series(
        "DIGITAL_CURRENCY_DAILY",
        "Time Series (Digital Currency Daily)",
        "alpha_vantage_crypto_daily",
        symbol=symbol,
        market=market,
        api_key=api_key,
    )


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"], secrets=("api_key",))
async def alpha_vantage_crypto_weekly(symbol: str, market: str = "USD", *, api_key: str = "") -> Any:
    """Fetch weekly OHLCV time series for a cryptocurrency. Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _crypto_series(
        "DIGITAL_CURRENCY_WEEKLY",
        "Time Series (Digital Currency Weekly)",
        "alpha_vantage_crypto_weekly",
        symbol=symbol,
        market=market,
        api_key=api_key,
    )


@connector(output=_CRYPTO_DAILY_OUTPUT, tags=["crypto"], secrets=("api_key",))
async def alpha_vantage_crypto_monthly(symbol: str, market: str = "USD", *, api_key: str = "") -> Any:
    """Fetch monthly OHLCV time series for a cryptocurrency. Returns full history.
    Free tier: 25 requests/day total across all endpoints.
    """
    return await _crypto_series(
        "DIGITAL_CURRENCY_MONTHLY",
        "Time Series (Digital Currency Monthly)",
        "alpha_vantage_crypto_monthly",
        symbol=symbol,
        market=market,
        api_key=api_key,
    )


# ---------------------------------------------------------------------------
# Economic Indicators
# ---------------------------------------------------------------------------


@connector(output=_ECON_OUTPUT, tags=["macro"], secrets=("api_key",))
async def alpha_vantage_econ(
    function: str,
    interval: Literal["daily", "weekly", "monthly", "quarterly", "annual"] | None = None,
    maturity: Literal["3month", "2year", "5year", "7year", "10year", "30year"] | None = None,
    *,
    api_key: str = "",
) -> Any:
    """Fetch US economic indicator time series. Covers real GDP, CPI, inflation,
    unemployment, federal funds rate, treasury yield (with maturity selection),
    retail sales, durables, and nonfarm payroll. Commodity data is available
    via the FRED connector instead (superior historical coverage).
    Use maturity param for TREASURY_YIELD (default 10year).
    Free tier: 25 requests/day total across all endpoints.
    """
    if function not in _ECON_FUNCTIONS:
        raise InvalidParameterError(
            _PROVIDER, f"function must be one of {', '.join(_ECON_FUNCTIONS)}; got {function!r}"
        )
    http = _client(api_key)
    req_params: dict[str, Any] = {}
    if interval is not None:
        req_params["interval"] = interval
    if maturity is not None and function == "TREASURY_YIELD":
        req_params["maturity"] = maturity

    data = await _av_fetch(
        http,
        function=function,
        params=req_params or None,
        op_name="alpha_vantage_econ",
    )

    observations = data.get("data", [])
    if not observations:
        raise EmptyDataError(_PROVIDER, query_params={"function": function})

    series_name = data.get("name", function)
    unit = data.get("unit", "")
    resp_interval = data.get("interval", "")

    rows = []
    for obs in observations:
        val = obs.get("value")
        if val == ".":
            val = None
        rows.append(
            {
                "name": function,
                "series_name": series_name,
                "date": obs.get("date"),
                "value": val,
                "unit": unit,
                "interval": resp_interval,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Precious Metals — Spot Price & Historical
# ---------------------------------------------------------------------------


@connector(output=_METAL_SPOT_OUTPUT, tags=["commodities"], secrets=("api_key",))
async def alpha_vantage_metal_spot(symbol: Literal["GOLD", "XAU", "SILVER", "XAG"], *, api_key: str = "") -> Any:
    """Fetch real-time spot price for gold or silver. Returns current price and
    timestamp. Use GOLD/XAU for gold, SILVER/XAG for silver.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="GOLD_SILVER_SPOT",
        params={"symbol": symbol},
        op_name="alpha_vantage_metal_spot",
    )

    if not isinstance(data, dict) or "price" not in data:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol})

    row = {
        "symbol": symbol,
        "nominal": data.get("nominal", symbol),
        "price": data.get("price"),
        "timestamp": data.get("timestamp", ""),
    }
    return pd.DataFrame([row])


@connector(output=_METAL_HISTORY_OUTPUT, tags=["commodities"], secrets=("api_key",))
async def alpha_vantage_metal_history(
    symbol: Literal["GOLD", "XAU", "SILVER", "XAG"],
    interval: Literal["daily", "weekly", "monthly"] = "monthly",
    *,
    api_key: str = "",
) -> Any:
    """Fetch historical prices for gold or silver. Returns date and price.
    Note: uses 'price' field (not 'value') unlike other commodity endpoints.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="GOLD_SILVER_HISTORY",
        params={"symbol": symbol, "interval": interval},
        op_name="alpha_vantage_metal_history",
    )

    observations = data.get("data", [])
    if not observations:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "interval": interval})

    rows = []
    for obs in observations:
        price = obs.get("price")
        if price == ".":
            price = None
        rows.append({"symbol": symbol, "date": obs.get("date"), "price": price})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Alpha Intelligence — News Sentiment
# ---------------------------------------------------------------------------


@connector(output=_NEWS_OUTPUT, tags=["news", "tool"], secrets=("api_key",))
async def alpha_vantage_news(
    tickers: str | None = None,
    topics: str | None = None,
    sort: Literal["LATEST", "EARLIEST", "RELEVANCE"] = "LATEST",
    limit: int = 50,
    *,
    api_key: str = "",
) -> Any:
    """Fetch news articles with sentiment scores. Filter by ticker(s) and/or
    topics. Each article includes title, summary, source, sentiment score
    (-1 to 1), and sentiment label. For ticker-specific sentiment, check the
    ticker_sentiment array in the raw response.
    Free tier: 25 requests/day total across all endpoints.
    """
    if not 1 <= limit <= 1000:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 1000")
    http = _client(api_key)
    req_params: dict[str, Any] = {"sort": sort, "limit": limit}
    if tickers:
        req_params["tickers"] = tickers
    if topics:
        req_params["topics"] = topics

    data = await _av_fetch(
        http,
        function="NEWS_SENTIMENT",
        params=req_params,
        op_name="alpha_vantage_news",
    )

    feed = data.get("feed", [])
    if not feed:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": tickers, "topics": topics})

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
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Alpha Intelligence — Top Gainers/Losers
# ---------------------------------------------------------------------------


@connector(output=_MOVERS_OUTPUT, tags=["equities", "tool"], secrets=("api_key",))
async def alpha_vantage_top_movers(*, api_key: str = "") -> Any:
    """Fetch today's top 20 gainers, top 20 losers, and top 20 most actively
    traded US equities. Each entry includes ticker, price, change amount,
    change percentage, and volume. No parameters required.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function="TOP_GAINERS_LOSERS",
        op_name="alpha_vantage_top_movers",
    )

    rows = []
    for category in ("top_gainers", "top_losers", "most_actively_traded"):
        for item in data.get(category, []):
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
        raise EmptyDataError(_PROVIDER, query_params={})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Options — Historical Options Chain (premium only)
# ---------------------------------------------------------------------------


@connector(output=_OPTIONS_OUTPUT, tags=["equities", "options"], secrets=("api_key",))
async def alpha_vantage_options(
    symbol: Annotated[str, "ns:alpha_vantage"],
    date: str | None = None,
    *,
    api_key: str = "",
) -> Any:
    """[Premium] Fetch historical options chain for a stock: contract ID,
    expiration, strike, type (call/put), last price, bid/ask, volume,
    open interest, implied volatility, and Greeks (delta, gamma, theta, vega).
    Requires a premium Alpha Vantage plan.
    """
    symbol = _require_nonempty(symbol, "symbol")
    http = _client(api_key)
    req_params: dict[str, Any] = {"symbol": symbol}
    if date:
        req_params["date"] = date

    data = await _av_fetch(
        http,
        function="HISTORICAL_OPTIONS",
        params=req_params,
        op_name="alpha_vantage_options",
    )

    contracts = data if isinstance(data, list) else data.get("data", [])
    if not contracts:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "date": date})
    return pd.DataFrame(contracts)


# ---------------------------------------------------------------------------
# Calendars — Earnings Calendar & IPO Calendar (CSV endpoints)
# ---------------------------------------------------------------------------


@connector(output=_EARNINGS_CAL_OUTPUT, tags=["equities", "calendars"], secrets=("api_key",))
async def alpha_vantage_earnings_calendar(
    horizon: Literal["3month", "6month", "12month"] = "3month",
    symbol: str | None = None,
    *,
    api_key: str = "",
) -> Any:
    """Fetch upcoming earnings release dates. Returns company name, report date,
    fiscal date ending, EPS estimate, and currency. Filter by symbol or get
    all upcoming earnings within the horizon window.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _client(api_key)
    req_params: dict[str, Any] = {"horizon": horizon}
    if symbol:
        req_params["symbol"] = symbol

    df = await _av_fetch_csv(
        http,
        function="EARNINGS_CALENDAR",
        params=req_params,
        op_name="alpha_vantage_earnings_calendar",
    )

    if df.empty:
        raise EmptyDataError(_PROVIDER, query_params={"horizon": horizon, "symbol": symbol})
    return df


@connector(output=_IPO_CAL_OUTPUT, tags=["equities", "calendars"], secrets=("api_key",))
async def alpha_vantage_ipo_calendar(*, api_key: str = "") -> Any:
    """Fetch upcoming and recent IPOs: company name, expected IPO date,
    price range (low/high), currency, and exchange.
    Free tier: 25 requests/day total across all endpoints.
    """
    http = _client(api_key)
    df = await _av_fetch_csv(
        http,
        function="IPO_CALENDAR",
        op_name="alpha_vantage_ipo_calendar",
    )

    if df.empty:
        raise EmptyDataError(_PROVIDER, query_params={})
    return df


# ---------------------------------------------------------------------------
# Technical Indicators (unified)
# ---------------------------------------------------------------------------


@connector(output=_TECHNICAL_OUTPUT, tags=["equities", "technical"], secrets=("api_key",))
async def alpha_vantage_technical(
    symbol: Annotated[str, "ns:alpha_vantage"],
    function: str,
    interval: Literal["1min", "5min", "15min", "30min", "60min", "daily", "weekly", "monthly"] = "daily",
    time_period: int = 20,
    series_type: Literal["close", "open", "high", "low"] = "close",
    *,
    api_key: str = "",
) -> Any:
    """Fetch a technical indicator for a stock. Supports 50+ indicators including
    SMA, EMA, RSI, MACD, Bollinger Bands, Stochastic, ADX, CCI, OBV, ATR, and more.
    The response columns vary by indicator (e.g. SMA returns 'SMA', BBANDS returns
    'Real Upper Band', 'Real Middle Band', 'Real Lower Band'). All values are numeric.
    Free tier: 25 requests/day total across all endpoints.
    """
    symbol = _require_nonempty(symbol, "symbol")
    if function not in _TECHNICAL_INDICATORS:
        raise InvalidParameterError(
            _PROVIDER, f"function must be a supported indicator; got {function!r}"
        )
    if time_period < 1:
        raise InvalidParameterError(_PROVIDER, "time_period must be >= 1")
    http = _client(api_key)
    data = await _av_fetch(
        http,
        function=function,
        params={
            "symbol": symbol,
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type,
        },
        op_name="alpha_vantage_technical",
    )

    ta_key = f"Technical Analysis: {function}"
    time_series = data.get(ta_key, {})
    if not time_series:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": symbol, "function": function})

    rows = []
    for date_str, values in time_series.items():
        row: dict[str, Any] = {"date": date_str, "symbol": symbol}
        row.update(values)
        rows.append(row)

    df = pd.DataFrame(rows)
    # Coerce only the indicator value columns to numeric — never the key columns.
    for col in df.columns:
        if col not in ("date", "symbol"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Listing Status — Enumerator
# ---------------------------------------------------------------------------

# Cap the enumerator output: LISTING_STATUS returns the FULL US-listing universe
# (thousands of rows) in one CSV. The catalog only needs a representative slice
# for discovery; an unbounded build would balloon memory and indexing cost.
_LISTING_MAX_ROWS = 5000


@enumerator(output=_LISTING_OUTPUT, tags=["equities"], secrets=("api_key",))
async def enumerate_alpha_vantage(
    state: Literal["active", "delisted"] = "active",
    *,
    api_key: str = "",
) -> pd.DataFrame:
    """Enumerate US-listed securities from Alpha Vantage for catalog indexing.

    Returns symbol, name, exchange, asset type (Stock/ETF), IPO date, and status.
    Use state='active' for current listings (default), 'delisted' for historical.
    Bounded to a head slice of the full listing universe.
    """
    http = _client(api_key)
    df = await _av_fetch_csv(
        http,
        function="LISTING_STATUS",
        params={"state": state},
        op_name="enumerate_alpha_vantage",
    )

    declared = [c.name for c in _LISTING_OUTPUT.columns]
    if df.empty:
        return pd.DataFrame(columns=declared)

    cols = [c for c in declared if c in df.columns]
    return df[cols].head(_LISTING_MAX_ROWS)
