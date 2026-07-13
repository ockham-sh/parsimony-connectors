"""FMP source: Financial Modeling Prep connector for the parsimony kernel.

API docs: https://site.financialmodelingprep.com/developer/docs
Authentication: ``apikey`` query parameter (in the transport sensitive-param
  set, so it is redacted from every log line and never reaches a surfaced URL).
Base URL: https://financialmodelingprep.com/stable

Exports 19 plain ``@connector`` verbs covering the FMP equity surface:
discovery, core market data, fundamentals, events and catalysts, signals,
market context, and the global equity screener.

The API key is declared as a secret (stripped from provenance) and supplied
either by binding (``load(api_key=...)`` / ``Connector.bind``) or, as a dev
fallback, from the ``FMP_API_KEY`` environment variable. A missing key fails
fast with :class:`UnauthorizedError` naming the env var.

Status semantics (verified live 2026-06-04): an invalid key returns **401**
(→ :class:`UnauthorizedError`); a plan / legacy restriction returns **403** (and
FMP also uses **402**) → :class:`PaymentRequiredError`. Several verbs are
plan-gated — their docstrings tag the minimum plan (``[All plans]``,
``[Starter+]``, ``[Professional+]``); on a too-low plan they return
:class:`PaymentRequiredError`.

Internal layout (not part of the public contract):

* :mod:`parsimony_fmp._http` — keyed client builder and error mapping.
* :mod:`parsimony_fmp.outputs` — declarative :class:`OutputSpec` schemas.
* :mod:`parsimony_fmp._screener` — the screener's classification frozensets,
  pushdown map, and fan-out orchestration.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_fmp import _screener
from parsimony_fmp._http import _BULK_TIMEOUT_SECONDS, _client, fmp_get
from parsimony_fmp.outputs import (
    ANALYST_ESTIMATES_OUTPUT,
    BALANCE_SHEET_OUTPUT,
    CASH_FLOW_OUTPUT,
    COMPANY_PROFILE_OUTPUT,
    EARNINGS_TRANSCRIPT_OUTPUT,
    HISTORICAL_PRICES_OUTPUT,
    INCOME_STATEMENT_OUTPUT,
    INDEX_CONSTITUENTS_OUTPUT,
    INSIDER_TRADES_OUTPUT,
    INSTITUTIONAL_POSITIONS_OUTPUT,
    MARKET_MOVERS_OUTPUT,
    NEWS_OUTPUT,
    PEERS_OUTPUT,
    SCREENER_OUTPUT,
    SEARCH_OUTPUT,
    STOCK_QUOTE_OUTPUT,
)

__all__ = ["CONNECTORS", "load"]

_PROVIDER = "fmp"


# ---------------------------------------------------------------------------
# Response shaping helpers
# ---------------------------------------------------------------------------


def _to_frame(data: Any, op_name: str, query_params: dict[str, Any]) -> pd.DataFrame:
    """Build a DataFrame from an FMP JSON list of records; guard empty/parse failures.

    FMP returns a JSON array for every list endpoint, and a 200 with ``[]`` for an
    unknown symbol — surfaced here as :class:`EmptyDataError`.
    """
    if isinstance(data, dict):
        # A couple of FMP endpoints wrap rows under an envelope key.
        for envelope_key in ("historical", "data", "results"):
            rows = data.get(envelope_key)
            if isinstance(rows, list):
                data = rows
                break
        else:
            data = [data]
    if not isinstance(data, list):
        raise ParseError(_PROVIDER, f"{op_name} response was not a JSON array")
    if not data:
        raise EmptyDataError(_PROVIDER, query_params=query_params)
    df = pd.DataFrame(data)
    if df.empty:
        raise EmptyDataError(_PROVIDER, query_params=query_params)
    return df


def _select_declared(df: pd.DataFrame, output: Any) -> pd.DataFrame:
    """Project a frame to the columns the schema declares, in declared order.

    Drops provider extras not in the schema. Missing declared columns are filled
    with ``NA`` so sparse upstream payloads still satisfy
    :class:`~parsimony.result.OutputSpec`. Wildcard (``"*"``) schemas keep
    unmapped columns after the fixed prefix.
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


# ---------------------------------------------------------------------------
# Dispatch maps (enum → upstream path).
# ---------------------------------------------------------------------------

_TAXONOMY_DISPATCH: dict[str, str] = {
    "sectors": "available-sectors",
    "industries": "available-industries",
    "exchanges": "available-exchanges",
    "symbols_with_financials": "stock-list",
}

_NEWS_DISPATCH: dict[str, str] = {
    "news": "news/stock",
    "press_releases": "news/press-releases",
}

_CORPORATE_HISTORY_DISPATCH: dict[str, str] = {
    "earnings": "earnings",
    "dividends": "dividends",
    "splits": "splits",
}

_EVENT_CALENDAR_DISPATCH: dict[str, str] = {
    "earnings": "earnings-calendar",
    "dividends": "dividends-calendar",
    "splits": "splits-calendar",
}

_INDEX_DISPATCH: dict[str, str] = {
    "SP500": "sp500-constituent",
    "NASDAQ": "nasdaq-constituent",
    "DOW_JONES": "dowjones-constituent",
}

_MARKET_MOVERS_DISPATCH: dict[str, str] = {
    "gainers": "biggest-gainers",
    "losers": "biggest-losers",
    "most_actives": "most-actives",
}

_INTRADAY_FREQUENCIES: frozenset[str] = frozenset({"1min", "5min", "15min", "30min", "1hour", "4hour"})

_PRICES_PATH_MAP: dict[str, str] = {
    "daily": "historical-price-eod/full",
    "dividend_adjusted": "historical-price-eod/dividend-adjusted",
}

# The dividend-adjusted route carries adjusted OHLC under different keys
# (verified live: adjOpen/adjHigh/adjLow/adjClose, no open/high/low/close).
# Rename them onto the declared schema before shaping, otherwise _select_declared
# drops every price column. The route has no change/changePercent/vwap; those
# stay absent (optional in the schema).
_DIVIDEND_ADJUSTED_RENAME: dict[str, str] = {
    "adjOpen": "open",
    "adjHigh": "high",
    "adjLow": "low",
    "adjClose": "close",
}


# ---------------------------------------------------------------------------
# Connectors — Discovery
# ---------------------------------------------------------------------------


@connector(output=SEARCH_OUTPUT, tags=["equity", "utility", "tool"], secrets=("api_key",))
def fmp_search(
    query: str,
    limit: int = 20,
    exchange: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[All plans] Search for companies by name fragment or partial ticker.

    Returns matches ranked by relevance (symbol, name, currency, exchange). Use
    to resolve a company name to its ticker symbol.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError(_PROVIDER, "query must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="search-name",
        params={"query": q, "limit": limit, "exchange": exchange},
        op_name="fmp_search",
    )
    df = _to_frame(data, "fmp_search", {"query": q})
    return _select_declared(df, SEARCH_OUTPUT)


@connector(tags=["equity", "utility", "tool"], secrets=("api_key",))
def fmp_taxonomy(
    type: Literal["sectors", "industries", "exchanges", "symbols_with_financials"],
    api_key: str = "",
) -> pd.DataFrame:
    """[Paid] Return valid values for a taxonomy type: sectors, industries,
    exchanges, or symbols_with_financials.

    Use before building screener filters to ensure field values are valid.

    Tier: despite FMP's docs implying these list endpoints are free, a live
    free-tier key gets HTTP 402 (paid-only, verified 2026-07-08) — hence
    [Paid], not [All plans]. Re-verify if FMP tiering changes.
    """
    http = _client(api_key, timeout=_BULK_TIMEOUT_SECONDS)
    data = fmp_get(http, path=_TAXONOMY_DISPATCH[type], op_name="fmp_taxonomy")
    return _to_frame(data, "fmp_taxonomy", {"type": type})


# ---------------------------------------------------------------------------
# Connectors — Core market data
# ---------------------------------------------------------------------------


@connector(output=STOCK_QUOTE_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_quotes(symbols: str, api_key: str = "") -> pd.DataFrame:
    """[Starter+] Fetch real-time quotes for one or more symbols in a single
    request.

    Returns price, change, day/52-week ranges, market cap, volume, and 50/200-day
    moving averages. Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    s = symbols.strip()
    if not s:
        raise InvalidParameterError(_PROVIDER, "symbols must be non-empty")
    http = _client(api_key)
    data = fmp_get(http, path="batch-quote", params={"symbols": s}, op_name="fmp_quotes")
    df = _to_frame(data, "fmp_quotes", {"symbols": s})
    return _select_declared(df, STOCK_QUOTE_OUTPUT)


@connector(output=HISTORICAL_PRICES_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_prices(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    frequency: str = "daily",
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Fetch price history for a symbol.

    Supports daily, dividend_adjusted, and intraday frequencies (1min, 5min,
    15min, 30min, 1hour, 4hour). Daily returns full OHLCV; intraday returns the
    last ~5 days. Intraday (1min-4hour) requires Professional tier.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    if frequency in _INTRADAY_FREQUENCIES:
        path = f"historical-chart/{frequency}"
    elif frequency in _PRICES_PATH_MAP:
        path = _PRICES_PATH_MAP[frequency]
    else:
        valid = sorted(set(_PRICES_PATH_MAP) | _INTRADAY_FREQUENCIES)
        raise InvalidParameterError(_PROVIDER, f"frequency must be one of {valid}, got {frequency!r}")

    http = _client(api_key)
    data = fmp_get(
        http,
        path=path,
        params={"symbol": sym, "from": from_date, "to": to_date},
        op_name="fmp_prices",
    )
    df = _to_frame(data, "fmp_prices", {"symbol": sym})
    if frequency == "dividend_adjusted":
        df = df.rename(columns=_DIVIDEND_ADJUSTED_RENAME)
    df = _select_declared(df, HISTORICAL_PRICES_OUTPUT)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------------------------------------------------------------------------
# Connectors — Fundamentals
# ---------------------------------------------------------------------------


@connector(output=COMPANY_PROFILE_OUTPUT, tags=["equity", "tool"], secrets=("api_key",))
def fmp_company_profile(symbol: Annotated[str, Namespace("fmp_symbols")], api_key: str = "") -> pd.DataFrame:
    """[Starter+] Fetch company profile: name, sector, industry, market cap, CEO,
    employees, website, and ETF/ADR/fund flags.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(http, path="profile", params={"symbol": sym}, op_name="fmp_company_profile")
    df = _to_frame(data, "fmp_company_profile", {"symbol": sym})
    return _select_declared(df, COMPANY_PROFILE_OUTPUT)


@connector(output=PEERS_OUTPUT, tags=["equity", "tool"], secrets=("api_key",))
def fmp_peers(symbol: Annotated[str, Namespace("fmp_symbols")], api_key: str = "") -> pd.DataFrame:
    """[Starter+] Return the peer group for a company: stocks in the same sector
    with comparable market cap on the same exchange.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(http, path="stock-peers", params={"symbol": sym}, op_name="fmp_peers")
    df = _to_frame(data, "fmp_peers", {"symbol": sym})
    return _select_declared(df, PEERS_OUTPUT)


@connector(output=INCOME_STATEMENT_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_income_statements(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    period: str = "annual",
    limit: int = 5,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Fetch income statements: revenue, gross profit, operating income,
    EBITDA, net income, and EPS over multiple periods.

    period is 'annual' or 'quarter'. Demo: 3 symbols (AAPL, TSLA, MSFT).
    Starter+: all symbols with multi-year history.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="income-statement",
        params={"symbol": sym, "period": period, "limit": limit},
        op_name="fmp_income_statements",
    )
    df = _to_frame(data, "fmp_income_statements", {"symbol": sym})
    df = _select_declared(df, INCOME_STATEMENT_OUTPUT)
    df["date"] = pd.to_datetime(df["date"])
    return df


@connector(output=BALANCE_SHEET_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_balance_sheet_statements(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    period: str = "annual",
    limit: int = 5,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Fetch balance sheets: assets, liabilities, equity, debt, and cash
    over multiple periods.

    period is 'annual' or 'quarter'. Demo: 3 symbols (AAPL, TSLA, MSFT).
    Starter+: all symbols with multi-year history.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="balance-sheet-statement",
        params={"symbol": sym, "period": period, "limit": limit},
        op_name="fmp_balance_sheet_statements",
    )
    df = _to_frame(data, "fmp_balance_sheet_statements", {"symbol": sym})
    df = _select_declared(df, BALANCE_SHEET_OUTPUT)
    df["date"] = pd.to_datetime(df["date"])
    return df


@connector(output=CASH_FLOW_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_cash_flow_statements(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    period: str = "annual",
    limit: int = 5,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Fetch cash flow statements: operating, investing, and financing
    activities plus free cash flow over multiple periods.

    period is 'annual' or 'quarter'. Demo: 3 symbols (AAPL, TSLA, MSFT).
    Starter+: all symbols with multi-year history.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="cash-flow-statement",
        params={"symbol": sym, "period": period, "limit": limit},
        op_name="fmp_cash_flow_statements",
    )
    df = _to_frame(data, "fmp_cash_flow_statements", {"symbol": sym})
    df = _select_declared(df, CASH_FLOW_OUTPUT)
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------------------------------------------------------------------------
# Connectors — Events and catalysts
# ---------------------------------------------------------------------------


@connector(tags=["equity"], secrets=("api_key",))
def fmp_corporate_history(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    event_type: Literal["earnings", "dividends", "splits"],
    limit: int = 10,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Fetch historical corporate events for a symbol: earnings (EPS and
    revenue actual vs estimated), dividends, or splits.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path=_CORPORATE_HISTORY_DISPATCH[event_type],
        params={"symbol": sym, "limit": limit},
        op_name="fmp_corporate_history",
    )
    return _to_frame(data, "fmp_corporate_history", {"symbol": sym, "event_type": event_type})


@connector(tags=["equity"], secrets=("api_key",))
def fmp_event_calendar(
    event_type: Literal["earnings", "dividends", "splits"],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[All plans] Return the market-wide calendar for earnings, dividends, or
    splits within a date window (max 90 days).
    """
    http = _client(api_key, timeout=_BULK_TIMEOUT_SECONDS)
    data = fmp_get(
        http,
        path=_EVENT_CALENDAR_DISPATCH[event_type],
        params={"from": from_date, "to": to_date},
        op_name="fmp_event_calendar",
    )
    return _to_frame(data, "fmp_event_calendar", {"event_type": event_type})


@connector(output=ANALYST_ESTIMATES_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_analyst_estimates(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    period: str = "annual",
    limit: int = 4,
    api_key: str = "",
) -> pd.DataFrame:
    """[Professional+] Fetch forward analyst consensus estimates: revenue, EBITDA,
    net income, and EPS low/avg/high plus analyst coverage counts.

    Requires Professional tier or above.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="analyst-estimates",
        params={"symbol": sym, "period": period, "limit": limit},
        op_name="fmp_analyst_estimates",
    )
    df = _to_frame(data, "fmp_analyst_estimates", {"symbol": sym})
    df = _select_declared(df, ANALYST_ESTIMATES_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Connectors — Signals and context
# ---------------------------------------------------------------------------


@connector(output=NEWS_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_news(
    type: Literal["news", "press_releases"],
    symbols: str,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 20,
    page: int = 0,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Fetch stock news articles or official press releases for one or
    more symbols.

    Pass type='news' for third-party articles or 'press_releases' for company IR
    communications. Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    s = symbols.strip()
    if not s:
        raise InvalidParameterError(_PROVIDER, "symbols must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path=_NEWS_DISPATCH[type],
        params={"symbols": s, "from": from_date, "to": to_date, "limit": limit, "page": page},
        op_name="fmp_news",
    )
    df = _to_frame(data, "fmp_news", {"symbols": s, "type": type})
    df = _select_declared(df, NEWS_OUTPUT)
    df["publishedDate"] = pd.to_datetime(df["publishedDate"])
    return df


@connector(output=INSIDER_TRADES_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_insider_trades(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    limit: int = 20,
    page: int = 0,
    api_key: str = "",
) -> pd.DataFrame:
    """[Professional+] Fetch insider trading activity (executive and director
    trades): transaction type, shares, price, and insider name.

    Requires Professional tier or above.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="insider-trading/search",
        params={"symbol": sym, "limit": limit, "page": page},
        op_name="fmp_insider_trades",
    )
    df = _to_frame(data, "fmp_insider_trades", {"symbol": sym})
    df = _select_declared(df, INSIDER_TRADES_OUTPUT)
    df["filingDate"] = pd.to_datetime(df["filingDate"])
    df["transactionDate"] = pd.to_datetime(df["transactionDate"])
    return df


@connector(output=INSTITUTIONAL_POSITIONS_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_institutional_positions(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    year: str,
    quarter: str,
    api_key: str = "",
) -> pd.DataFrame:
    """[Professional+] Fetch a quarterly institutional (13F) ownership snapshot:
    investor count, share changes, invested value, and ownership %.

    quarter is 1, 2, 3, or 4. Requires Professional tier or above.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    if quarter not in {"1", "2", "3", "4"}:
        raise InvalidParameterError(_PROVIDER, f"quarter must be one of 1, 2, 3, 4, got {quarter!r}")
    http = _client(api_key)
    data = fmp_get(
        http,
        path="institutional-ownership/symbol-positions-summary",
        params={"symbol": sym, "year": year, "quarter": quarter},
        op_name="fmp_institutional_positions",
    )
    df = _to_frame(data, "fmp_institutional_positions", {"symbol": sym, "year": year, "quarter": quarter})
    df = _select_declared(df, INSTITUTIONAL_POSITIONS_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


@connector(output=EARNINGS_TRANSCRIPT_OUTPUT, tags=["equity"], secrets=("api_key",))
def fmp_earnings_transcript(
    symbol: Annotated[str, Namespace("fmp_symbols")],
    year: str,
    quarter: str,
    api_key: str = "",
) -> pd.DataFrame:
    """[Professional+] Fetch the full text transcript of an earnings call for a
    symbol, year, and quarter.

    quarter is 1, 2, 3, or 4. Requires Professional tier or above.
    """
    sym = symbol.strip()
    if not sym:
        raise InvalidParameterError(_PROVIDER, "symbol must be non-empty")
    if quarter not in {"1", "2", "3", "4"}:
        raise InvalidParameterError(_PROVIDER, f"quarter must be one of 1, 2, 3, 4, got {quarter!r}")
    http = _client(api_key, timeout=_BULK_TIMEOUT_SECONDS)
    data = fmp_get(
        http,
        path="earning-call-transcript",
        params={"symbol": sym, "year": year, "quarter": quarter},
        op_name="fmp_earnings_transcript",
    )
    df = _to_frame(data, "fmp_earnings_transcript", {"symbol": sym, "year": year, "quarter": quarter})
    df = _select_declared(df, EARNINGS_TRANSCRIPT_OUTPUT)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


# ---------------------------------------------------------------------------
# Connectors — Market context
# ---------------------------------------------------------------------------


@connector(output=INDEX_CONSTITUENTS_OUTPUT, tags=["equity", "tool"], secrets=("api_key",))
def fmp_index_constituents(
    index: Literal["SP500", "NASDAQ", "DOW_JONES"],
    api_key: str = "",
) -> pd.DataFrame:
    """[All plans] Return current constituents of SP500, NASDAQ, or DOW_JONES:
    symbol, name, sector, sub-sector, and headquarters.
    """
    http = _client(api_key)
    data = fmp_get(http, path=_INDEX_DISPATCH[index], op_name="fmp_index_constituents")
    df = _to_frame(data, "fmp_index_constituents", {"index": index})
    df = _select_declared(df, INDEX_CONSTITUENTS_OUTPUT)
    df["dateFirstAdded"] = pd.to_datetime(df["dateFirstAdded"]).dt.normalize()
    return df


@connector(output=MARKET_MOVERS_OUTPUT, tags=["equity", "tool"], secrets=("api_key",))
def fmp_market_movers(
    type: Literal["gainers", "losers", "most_actives"],
    api_key: str = "",
) -> pd.DataFrame:
    """[All plans] Return today's biggest market movers: gainers (highest % up),
    losers (biggest % down), or most_actives (highest volume).
    """
    http = _client(api_key)
    data = fmp_get(http, path=_MARKET_MOVERS_DISPATCH[type], op_name="fmp_market_movers")
    df = _to_frame(data, "fmp_market_movers", {"type": type})
    return _select_declared(df, MARKET_MOVERS_OUTPUT)


# ---------------------------------------------------------------------------
# Connectors — Global equity screener (fans out to 3 FMP endpoints, joins,
# post-filters). Thin stub — orchestration lives in :mod:`parsimony_fmp._screener`.
# ---------------------------------------------------------------------------


@connector(output=SCREENER_OUTPUT, tags=["equity", "tool"], secrets=("api_key",))
def fmp_screener(
    sector: str | None = None,
    industry: str | None = None,
    country: str | None = None,
    exchange: str | None = None,
    market_cap_min: float | None = None,
    market_cap_max: float | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    volume_min: float | None = None,
    volume_max: float | None = None,
    beta_min: float | None = None,
    beta_max: float | None = None,
    dividend_min: float | None = None,
    dividend_max: float | None = None,
    is_etf: bool | None = None,
    is_fund: bool | None = None,
    is_actively_trading: bool | None = None,
    where_clause: str | None = None,
    sort_by: str | None = None,
    sort_order: str = "desc",
    limit: int = 100,
    prefilter_limit: int | None = None,
    fields: list[str] | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """[Starter+] Screen the global equity universe by financial metrics.

    Use pushdown params (sector, country, market_cap_min, etc.) to narrow the
    universe, then where_clause for residual conditions on enriched TTM metrics
    (ratios, yields, multiples, margins). Enriches with key-metrics-ttm and
    financial-ratios-ttm. Use fields to restrict output and skip unnecessary
    enrichment. Use sort_by + limit for top-N. Increase prefilter_limit
    (1000-2000) for broad global searches sorted by TTM columns.
    """
    http = _client(api_key)
    return _screener.execute(
        http,
        sector=sector,
        industry=industry,
        country=country,
        exchange=exchange,
        market_cap_min=market_cap_min,
        market_cap_max=market_cap_max,
        price_min=price_min,
        price_max=price_max,
        volume_min=volume_min,
        volume_max=volume_max,
        beta_min=beta_min,
        beta_max=beta_max,
        dividend_min=dividend_min,
        dividend_max=dividend_max,
        is_etf=is_etf,
        is_fund=is_fund,
        is_actively_trading=is_actively_trading,
        where_clause=where_clause,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        prefilter_limit=prefilter_limit,
        fields=fields,
    )


CONNECTORS = Connectors(
    [
        # Discovery
        fmp_search,
        fmp_taxonomy,
        # Core market data
        fmp_quotes,
        fmp_prices,
        # Fundamentals
        fmp_company_profile,
        fmp_peers,
        fmp_income_statements,
        fmp_balance_sheet_statements,
        fmp_cash_flow_statements,
        # Events and catalysts
        fmp_corporate_history,
        fmp_event_calendar,
        fmp_analyst_estimates,
        # Signals and context
        fmp_news,
        fmp_insider_trades,
        fmp_institutional_positions,
        fmp_earnings_transcript,
        # Market context
        fmp_index_constituents,
        fmp_market_movers,
        # Screener
        fmp_screener,
    ]
)


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
