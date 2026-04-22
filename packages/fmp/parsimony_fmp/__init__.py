"""FMP source: Financial Modeling Prep connector for the parsimony kernel.

Exports 19 connectors covering the full FMP equity surface: discovery,
core market data, fundamentals, events and catalysts, signals, market
context, and the global equity screener.

Internal layout (not part of the public contract):

* :mod:`parsimony_fmp._http` — shared transport, unified error mapping,
  URL redaction, pooled-client context manager.
* :mod:`parsimony_fmp.params` — Pydantic parameter models for every
  ``@connector`` function.
* :mod:`parsimony_fmp.outputs` — declarative :class:`OutputConfig`
  schemas for every DataFrame-returning connector.
* :mod:`parsimony_fmp._screener` — the screener's classification
  frozensets, pushdown map, and fan-out orchestration.
"""

from __future__ import annotations

from typing import Any

from parsimony.connector import Connectors, connector
from parsimony.result import Result

from parsimony_fmp import _screener
from parsimony_fmp._http import fmp_fetch, make_http
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
from parsimony_fmp.params import (
    FmpAnalystEstimatesParams,
    FmpCorporateHistoryParams,
    FmpEarningsTranscriptParams,
    FmpEventCalendarParams,
    FmpFinancialStatementParams,
    FmpHistoricalPricesParams,
    FmpIndexConstituentsParams,
    FmpInsiderTradesParams,
    FmpInstitutionalPositionsParams,
    FmpMarketMoversParams,
    FmpNewsParams,
    FmpScreenerParams,
    FmpSearchParams,
    FmpSymbolParams,
    FmpSymbolsParams,
    FmpTaxonomyParams,
)

_DEFAULT_BASE_URL = "https://financialmodelingprep.com/stable"


# ---------------------------------------------------------------------------
# Dispatch maps (enum → upstream path). Collocated with the connectors that
# consume them so the "what values does this accept?" question is answered
# by grepping one file.
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

_INTRADAY_FREQUENCIES: frozenset[str] = frozenset(
    {"1min", "5min", "15min", "30min", "1hour", "4hour"}
)

_PRICES_PATH_MAP: dict[str, str] = {
    "daily": "historical-price-eod/full",
    "dividend_adjusted": "historical-price-eod/dividend-adjusted",
}


# ---------------------------------------------------------------------------
# Connectors — Utilities
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, output=SEARCH_OUTPUT, tags=["equity", "utility", "tool"])
async def fmp_search(
    params: FmpSearchParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[All plans] Search for companies by name fragment or partial ticker.

    Returns matches ranked by relevance. Use to resolve a company name
    to its ticker symbol.
    """
    http = make_http(api_key, base_url)
    p: dict[str, Any] = {"query": params.query, "limit": params.limit}
    if params.exchange:
        p["exchange"] = params.exchange
    return await fmp_fetch(
        http,
        path="search-name",
        params=p,
        op_name="fmp_search",
        output_config=SEARCH_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, tags=["equity", "utility", "tool"])
async def fmp_taxonomy(
    params: FmpTaxonomyParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[All plans] Return valid values for a taxonomy type: sectors, industries,
    exchanges, or symbols_with_financials.

    Use before building screener filters to ensure field values are valid.
    """
    http = make_http(api_key, base_url)
    path = _TAXONOMY_DISPATCH[params.type]
    return await fmp_fetch(
        http,
        path=path,
        params={},
        op_name="fmp_taxonomy",
    )


# ---------------------------------------------------------------------------
# Connectors — Core market data
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, output=STOCK_QUOTE_OUTPUT, tags=["equity"])
async def fmp_quotes(
    params: FmpSymbolsParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch real-time quotes for one or more symbols in a single
    request.

    Returns price, change, 52-week range, market cap, volume, moving
    averages. Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="batch-quote",
        params={"symbols": params.symbols},
        op_name="fmp_quotes",
        output_config=STOCK_QUOTE_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=HISTORICAL_PRICES_OUTPUT, tags=["equity"])
async def fmp_prices(
    params: FmpHistoricalPricesParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch price history for a symbol.

    Supports daily, dividend_adjusted, and intraday frequencies (1min,
    5min, 15min, 30min, 1hour, 4hour). Daily returns full OHLCV +
    adjClose; intraday returns last ~5 days. Intraday (1min-4hour)
    requires Professional tier.
    """
    http = make_http(api_key, base_url)
    freq = params.frequency

    if freq in _INTRADAY_FREQUENCIES:
        path = f"historical-chart/{freq}"
    else:
        path = _PRICES_PATH_MAP.get(freq, "historical-price-eod/full")

    p: dict[str, Any] = {"symbol": params.symbol}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date

    return await fmp_fetch(
        http,
        path=path,
        params=p,
        op_name="fmp_prices",
        output_config=HISTORICAL_PRICES_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Fundamentals
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, output=COMPANY_PROFILE_OUTPUT, tags=["equity", "tool"])
async def fmp_company_profile(
    params: FmpSymbolParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch company profile: name, sector, industry, market cap,
    CEO, employees, website, ETF/ADR flags.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="profile",
        params={"symbol": params.symbol},
        op_name="fmp_company_profile",
        output_config=COMPANY_PROFILE_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=PEERS_OUTPUT, tags=["equity", "tool"])
async def fmp_peers(
    params: FmpSymbolParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Return the peer group for a company.

    Stocks in the same sector with comparable market cap on the same
    exchange. Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="stock-peers",
        params={"symbol": params.symbol},
        op_name="fmp_peers",
        output_config=PEERS_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=INCOME_STATEMENT_OUTPUT, tags=["equity"])
async def fmp_income_statements(
    params: FmpFinancialStatementParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch income statements: revenue, EBITDA, net income, EPS
    for multiple periods.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols with
    multi-year history.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="income-statement",
        params=params.model_dump(),
        op_name="fmp_income_statements",
        output_config=INCOME_STATEMENT_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=BALANCE_SHEET_OUTPUT, tags=["equity"])
async def fmp_balance_sheet_statements(
    params: FmpFinancialStatementParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch balance sheet: assets, liabilities, equity, debt, cash
    for multiple periods.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols with
    multi-year history.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="balance-sheet-statement",
        params=params.model_dump(),
        op_name="fmp_balance_sheet_statements",
        output_config=BALANCE_SHEET_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=CASH_FLOW_OUTPUT, tags=["equity"])
async def fmp_cash_flow_statements(
    params: FmpFinancialStatementParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch cash flow statement: operating, investing, financing
    activities, free cash flow for multiple periods.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols with
    multi-year history.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="cash-flow-statement",
        params=params.model_dump(),
        op_name="fmp_cash_flow_statements",
        output_config=CASH_FLOW_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Events and catalysts
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, tags=["equity"])
async def fmp_corporate_history(
    params: FmpCorporateHistoryParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch historical corporate events for a symbol: earnings
    (EPS, revenue actual vs estimated), dividends, or splits.

    Demo: 3 symbols (AAPL, TSLA, MSFT). Starter+: all symbols.
    """
    http = make_http(api_key, base_url)
    path = _CORPORATE_HISTORY_DISPATCH[params.event_type]
    return await fmp_fetch(
        http,
        path=path,
        params={"symbol": params.symbol, "limit": params.limit},
        op_name="fmp_corporate_history",
    )


@connector(env={"api_key": "FMP_API_KEY"}, tags=["equity"])
async def fmp_event_calendar(
    params: FmpEventCalendarParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[All plans] Return the market-wide calendar for earnings, dividends,
    or splits within a date window (max 90 days).
    """
    http = make_http(api_key, base_url)
    path = _EVENT_CALENDAR_DISPATCH[params.event_type]
    p: dict[str, Any] = {}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await fmp_fetch(
        http,
        path=path,
        params=p,
        op_name="fmp_event_calendar",
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=ANALYST_ESTIMATES_OUTPUT, tags=["equity"])
async def fmp_analyst_estimates(
    params: FmpAnalystEstimatesParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Professional+] Fetch forward analyst consensus estimates: revenue,
    EBITDA, net income, EPS low/avg/high plus analyst coverage counts.

    Requires Professional tier or above.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="analyst-estimates",
        params={"symbol": params.symbol, "period": params.period, "limit": params.limit},
        op_name="fmp_analyst_estimates",
        output_config=ANALYST_ESTIMATES_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Signals and context
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, output=NEWS_OUTPUT, tags=["equity"])
async def fmp_news(
    params: FmpNewsParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Fetch stock news articles or official press releases for
    one or more symbols.

    Pass type='news' for third-party articles or 'press_releases' for
    company IR communications. Demo: 3 symbols (AAPL, TSLA, MSFT).
    Starter+: all symbols.
    """
    http = make_http(api_key, base_url)
    path = _NEWS_DISPATCH[params.type]
    p: dict[str, Any] = {"symbols": params.symbols, "limit": params.limit, "page": params.page}
    if params.from_date:
        p["from"] = params.from_date
    if params.to_date:
        p["to"] = params.to_date
    return await fmp_fetch(
        http,
        path=path,
        params=p,
        op_name="fmp_news",
        output_config=NEWS_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=INSIDER_TRADES_OUTPUT, tags=["equity"])
async def fmp_insider_trades(
    params: FmpInsiderTradesParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Professional+] Fetch insider trading activity (executive and director
    trades): transaction type, shares, price, insider name.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="insider-trading/search",
        params={"symbol": params.symbol, "limit": params.limit, "page": params.page},
        op_name="fmp_insider_trades",
        output_config=INSIDER_TRADES_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=INSTITUTIONAL_POSITIONS_OUTPUT, tags=["equity"])
async def fmp_institutional_positions(
    params: FmpInstitutionalPositionsParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Professional+] Fetch quarterly institutional (13F) ownership snapshot:
    investor count, share changes, invested value, ownership %.
    """
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="institutional-ownership/symbol-positions-summary",
        params={"symbol": params.symbol, "year": params.year, "quarter": params.quarter},
        op_name="fmp_institutional_positions",
        output_config=INSTITUTIONAL_POSITIONS_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=EARNINGS_TRANSCRIPT_OUTPUT, tags=["equity"])
async def fmp_earnings_transcript(
    params: FmpEarningsTranscriptParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Professional+] Fetch the full text transcript of an earnings call for a symbol, year, and quarter."""
    http = make_http(api_key, base_url)
    return await fmp_fetch(
        http,
        path="earning-call-transcript",
        params={"symbol": params.symbol, "year": params.year, "quarter": params.quarter},
        op_name="fmp_earnings_transcript",
        output_config=EARNINGS_TRANSCRIPT_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Market context
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, output=INDEX_CONSTITUENTS_OUTPUT, tags=["equity", "tool"])
async def fmp_index_constituents(
    params: FmpIndexConstituentsParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[All plans] Return current constituents of SP500, NASDAQ, or DOW_JONES: symbol, name, sector, headquarters."""
    http = make_http(api_key, base_url)
    path = _INDEX_DISPATCH[params.index]
    return await fmp_fetch(
        http,
        path=path,
        params={},
        op_name="fmp_index_constituents",
        output_config=INDEX_CONSTITUENTS_OUTPUT,
    )


@connector(env={"api_key": "FMP_API_KEY"}, output=MARKET_MOVERS_OUTPUT, tags=["equity", "tool"])
async def fmp_market_movers(
    params: FmpMarketMoversParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[All plans] Return today's biggest market movers: gainers (highest %
    up), losers (biggest % down), or most_actives (highest volume).
    """
    http = make_http(api_key, base_url)
    path = _MARKET_MOVERS_DISPATCH[params.type]
    return await fmp_fetch(
        http,
        path=path,
        params={},
        op_name="fmp_market_movers",
        output_config=MARKET_MOVERS_OUTPUT,
    )


# ---------------------------------------------------------------------------
# Connectors — Global equity screener (fans out to 3 FMP endpoints, joins,
# post-filters). Thin stub — orchestration lives in :mod:`parsimony_fmp._screener`.
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FMP_API_KEY"}, output=SCREENER_OUTPUT, tags=["equity", "tool"])
async def fmp_screener(
    params: FmpScreenerParams,
    *,
    api_key: str,
    base_url: str = _DEFAULT_BASE_URL,
) -> Result:
    """[Starter+] Screen the global equity universe by financial metrics.

    Use pushdown params (sector, country, market_cap_min, etc.) to narrow the
    universe, then where_clause for residual conditions on enriched TTM metrics
    (ratios, yields, multiples, margins). Enriches with key-metrics-ttm and
    financial-ratios-ttm. Use fields to restrict output and skip unnecessary
    enrichment. Use sort_by + limit for top-N. Increase prefilter_limit
    (1000-2000) for broad global searches sorted by TTM columns.
    """
    http = make_http(api_key, base_url)
    return await _screener.execute(params, http)


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


__all__ = [
    "CONNECTORS",
    # Parameter models (public — downstream callers type against these)
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
    # Connector functions
    "fmp_analyst_estimates",
    "fmp_balance_sheet_statements",
    "fmp_cash_flow_statements",
    "fmp_company_profile",
    "fmp_corporate_history",
    "fmp_earnings_transcript",
    "fmp_event_calendar",
    "fmp_income_statements",
    "fmp_index_constituents",
    "fmp_insider_trades",
    "fmp_institutional_positions",
    "fmp_market_movers",
    "fmp_news",
    "fmp_peers",
    "fmp_prices",
    "fmp_quotes",
    "fmp_screener",
    "fmp_search",
    "fmp_taxonomy",
]
