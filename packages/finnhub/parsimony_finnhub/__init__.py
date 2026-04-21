"""Finnhub source: equity quotes, company fundamentals, news, and calendars.

API docs: https://finnhub.io/docs/api
Authentication: ``X-Finnhub-Token`` request header.
Base URL: https://finnhub.io/api/v1
Rate limit: 60 calls/min (free tier). Headers: X-Ratelimit-Remaining,
  X-Ratelimit-Reset (unix timestamp).

Provides 12 connectors:
  - Discovery: symbol search
  - Market data: real-time quote
  - Company: profile, peers, analyst recommendations, historical earnings
  - News: company-specific news, market-wide news
  - Fundamentals: basic financials (metrics + time series)
  - Calendars: earnings calendar, IPO calendar
  - Enumerator: full US symbol list for catalog indexing

Premium-only endpoints (403 on free tier): /stock/candle, /forex/rates,
  /stock/splits, /stock/dividend, /stock/price-target.

Internal layout (not part of the public contract):

* :mod:`parsimony_finnhub._http` — shared transport, unified error mapping,
  ``Retry-After`` parsing, JSON fetch helper.
* :mod:`parsimony_finnhub.params` — Pydantic parameter models.
* :mod:`parsimony_finnhub.outputs` — declarative :class:`OutputConfig`
  schemas.

This ``__init__.py`` stays at the top level so ``tools/gen_registry.py``
can AST-parse ``@connector`` decorators (it does not follow re-exports).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Provenance, Result
from parsimony.transport import HttpClient

from parsimony_finnhub._http import finnhub_fetch as _fh_fetch
from parsimony_finnhub._http import make_http as _make_http
from parsimony_finnhub.outputs import EARNINGS_CAL_OUTPUT as _EARNINGS_CAL_OUTPUT
from parsimony_finnhub.outputs import EARNINGS_OUTPUT as _EARNINGS_OUTPUT
from parsimony_finnhub.outputs import ENUMERATE_OUTPUT as _ENUMERATE_OUTPUT
from parsimony_finnhub.outputs import IPO_CAL_OUTPUT as _IPO_CAL_OUTPUT
from parsimony_finnhub.outputs import NEWS_OUTPUT as _NEWS_OUTPUT
from parsimony_finnhub.outputs import PEERS_OUTPUT as _PEERS_OUTPUT
from parsimony_finnhub.outputs import QUOTE_OUTPUT as _QUOTE_OUTPUT
from parsimony_finnhub.outputs import RECOMMENDATION_OUTPUT as _RECOMMENDATION_OUTPUT
from parsimony_finnhub.outputs import SEARCH_OUTPUT as _SEARCH_OUTPUT
from parsimony_finnhub.params import (
    FinnhubBasicFinancialsParams,
    FinnhubCompanyNewsParams,
    FinnhubEarningsCalendarParams,
    FinnhubEarningsParams,
    FinnhubEnumerateParams,
    FinnhubIpoCalendarParams,
    FinnhubMarketNewsParams,
    FinnhubPeersParams,
    FinnhubProfileParams,
    FinnhubQuoteParams,
    FinnhubRecommendationParams,
    FinnhubSearchParams,
)

ENV_VARS: dict[str, str] = {"api_key": "FINNHUB_API_KEY"}

_PROVIDER = "finnhub"

# Used by ``enumerate_finnhub`` below, which constructs its own
# ``HttpClient`` with a longer timeout and redirect following because the
# ``/stock/symbol`` endpoint is a large CDN-served static file.
_BASE_URL = "https://finnhub.io/api/v1"


# ---------------------------------------------------------------------------
# Discovery — Symbol Search
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["equities", "tool"])
async def finnhub_search(params: FinnhubSearchParams, *, api_key: str) -> Result:
    """Search Finnhub for stocks, ETFs, and indices by name or ticker symbol.
    Returns symbol (the stable API identifier), description (company name),
    displaySymbol, and type. Use symbol with finnhub_quote, finnhub_profile,
    or finnhub_company_news. Returns US and international symbols.

    Example: query='apple' → symbol='AAPL'; query='tesla' → symbol='TSLA'.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(http, path="/search", params={"q": params.query}, op_name="finnhub_search")

    results = data.get("result", [])
    if not results:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No symbols found for query: {params.query}",
        )

    rows = [
        {
            "symbol": r.get("symbol", ""),
            "description": r.get("description", ""),
            "display_symbol": r.get("displaySymbol", ""),
            "type": r.get("type", ""),
        }
        for r in results
        if r.get("symbol")
    ]
    df = pd.DataFrame(rows)
    return _SEARCH_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="finnhub_search", params={"query": params.query}),
        params={"query": params.query},
    )


# ---------------------------------------------------------------------------
# Market Data — Real-time Quote
# ---------------------------------------------------------------------------


@connector(output=_QUOTE_OUTPUT, tags=["equities"])
async def finnhub_quote(params: FinnhubQuoteParams, *, api_key: str) -> Result:
    """Fetch real-time quote for a stock: current price, day high/low/open,
    previous close, and absolute/percent change vs prior close. Timestamp
    is day-granularity (last close time, not a tick timestamp). Use
    finnhub_search to resolve ticker symbols first.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(http, path="/quote", params={"symbol": params.symbol}, op_name="finnhub_quote")

    if not isinstance(data, dict) or data.get("c") is None:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No quote data returned for symbol: {params.symbol}",
        )

    row = {
        "symbol": params.symbol,
        "current_price": data.get("c"),
        "change": data.get("d"),
        "change_percent": data.get("dp"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "timestamp": data.get("t"),
    }
    df = pd.DataFrame([row])
    return _QUOTE_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="finnhub_quote", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


# ---------------------------------------------------------------------------
# Company — Profile, Peers, Recommendations, Earnings, Fundamentals
# ---------------------------------------------------------------------------


@connector(tags=["equities"])
async def finnhub_profile(params: FinnhubProfileParams, *, api_key: str) -> Result:
    """Fetch company profile for a stock: name, exchange, country, currency,
    IPO date, industry, market cap (in millions USD), shares outstanding (millions),
    website, phone, and logo URL. Use finnhub_search to resolve ticker symbols.
    For time-series fundamentals use finnhub_basic_financials.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(http, path="/stock/profile2", params={"symbol": params.symbol}, op_name="finnhub_profile")

    if not isinstance(data, dict) or not data.get("name"):
        raise EmptyDataError(
            provider="finnhub",
            message=f"No profile data returned for symbol: {params.symbol}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="finnhub_profile", params={"symbol": params.symbol}),
    )


@connector(output=_PEERS_OUTPUT, tags=["equities"])
async def finnhub_peers(params: FinnhubPeersParams, *, api_key: str) -> Result:
    """Fetch peer/comparable companies for a stock. Returns a list of ticker
    symbols in the same industry and market cap range. Use finnhub_quote or
    finnhub_profile on returned symbols for further analysis.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(http, path="/stock/peers", params={"symbol": params.symbol}, op_name="finnhub_peers")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No peer data returned for symbol: {params.symbol}",
        )

    df = pd.DataFrame({"symbol": [s for s in data if isinstance(s, str)]})
    return _PEERS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="finnhub_peers", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


@connector(output=_RECOMMENDATION_OUTPUT, tags=["equities"])
async def finnhub_recommendation(params: FinnhubRecommendationParams, *, api_key: str) -> Result:
    """Fetch analyst buy/hold/sell recommendation trends for a stock.
    Returns monthly aggregated counts: strongBuy, buy, hold, sell, strongSell.
    Free tier returns approximately the last 4 months of data.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(
        http,
        path="/stock/recommendation",
        params={"symbol": params.symbol},
        op_name="finnhub_recommendation",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No recommendation data returned for symbol: {params.symbol}",
        )

    rows = [
        {
            "period": r.get("period"),
            "strong_buy": r.get("strongBuy"),
            "buy": r.get("buy"),
            "hold": r.get("hold"),
            "sell": r.get("sell"),
            "strong_sell": r.get("strongSell"),
        }
        for r in data
        if r.get("period")
    ]
    if not rows:
        raise EmptyDataError(provider="finnhub", message=f"Empty recommendation list for: {params.symbol}")

    df = pd.DataFrame(rows)
    return _RECOMMENDATION_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="finnhub_recommendation", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


@connector(output=_EARNINGS_OUTPUT, tags=["equities"])
async def finnhub_earnings(params: FinnhubEarningsParams, *, api_key: str) -> Result:
    """Fetch historical earnings per share (EPS) for a stock: actual EPS,
    consensus estimate, surprise, and surprise percent for the last ~4 quarters.
    For forward-looking earnings dates use finnhub_earnings_calendar.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(http, path="/stock/earnings", params={"symbol": params.symbol}, op_name="finnhub_earnings")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No earnings data returned for symbol: {params.symbol}",
        )

    rows = [
        {
            "period": r.get("period"),
            "quarter": r.get("quarter"),
            "year": r.get("year"),
            "eps_actual": r.get("actual"),
            "eps_estimate": r.get("estimate"),
            "eps_surprise": r.get("surprise"),
            "eps_surprise_percent": r.get("surprisePercent"),
        }
        for r in data
        if r.get("period")
    ]
    if not rows:
        raise EmptyDataError(provider="finnhub", message=f"Empty earnings list for: {params.symbol}")

    df = pd.DataFrame(rows)
    return _EARNINGS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="finnhub_earnings", params={"symbol": params.symbol}),
        params={"symbol": params.symbol},
    )


@connector(tags=["equities"])
async def finnhub_basic_financials(params: FinnhubBasicFinancialsParams, *, api_key: str) -> Result:
    """Fetch ~120 fundamental metrics for a stock: PE, EPS, beta, 52-week
    high/low, gross margin, ROE, dividend yield, market cap, and more.
    Also includes annual and quarterly time-series (going back to ~2007 for
    mature companies) for 37+ metrics: book value, EV/EBITDA, net margin, etc.
    Each time-series entry has period (ISO date) and v (float value).
    Response is a large dict with 'metric' (flat KPIs) and 'series' (time series).
    """
    http = _make_http(api_key)
    data = await _fh_fetch(
        http,
        path="/stock/metric",
        params={"symbol": params.symbol, "metric": "all"},
        op_name="finnhub_basic_financials",
    )

    if not isinstance(data, dict) or not data.get("metric"):
        raise EmptyDataError(
            provider="finnhub",
            message=f"No fundamental data returned for symbol: {params.symbol}",
        )

    return Result(
        data=data,
        provenance=Provenance(source="finnhub_basic_financials", params={"symbol": params.symbol}),
    )


# ---------------------------------------------------------------------------
# News — Company and Market
# ---------------------------------------------------------------------------


@connector(output=_NEWS_OUTPUT, tags=["equities", "news"])
async def finnhub_company_news(params: FinnhubCompanyNewsParams, *, api_key: str) -> Result:
    """Fetch news articles for a specific company between two dates.
    Returns headline, source, publish datetime (unix timestamp), summary,
    URL, and related ticker. Free tier access is limited to recent months —
    historical dates silently return empty results (no error raised).
    Note: URL may be a Finnhub proxy redirect rather than a direct publisher link.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(
        http,
        path="/company-news",
        params={"symbol": params.symbol, "from": params.from_date, "to": params.to_date},
        op_name="finnhub_company_news",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No news found for {params.symbol} between {params.from_date} and {params.to_date}",
        )

    rows = [
        {
            "id": item.get("id"),
            "datetime": item.get("datetime"),
            "headline": item.get("headline", ""),
            "source": item.get("source", ""),
            "category": item.get("category", ""),
            "related": item.get("related", ""),
            "summary": item.get("summary", ""),
            "url": item.get("url", ""),
            "image": item.get("image", ""),
        }
        for item in data
    ]
    df = pd.DataFrame(rows)
    return _NEWS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="finnhub_company_news",
            params={"symbol": params.symbol, "from": params.from_date, "to": params.to_date},
        ),
        params={"symbol": params.symbol},
    )


@connector(output=_NEWS_OUTPUT, tags=["news"])
async def finnhub_market_news(params: FinnhubMarketNewsParams, *, api_key: str) -> Result:
    """Fetch latest market-wide news by category. Categories: 'general' (top
    business/market headlines), 'forex', 'crypto', 'merger'. Returns up to
    ~100 articles. The 'related' field is empty for market news (unlike company
    news). For company-specific articles use finnhub_company_news.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(http, path="/news", params={"category": params.category}, op_name="finnhub_market_news")

    if not isinstance(data, list) or not data:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No market news returned for category: {params.category}",
        )

    rows = [
        {
            "id": item.get("id"),
            "datetime": item.get("datetime"),
            "headline": item.get("headline", ""),
            "source": item.get("source", ""),
            "category": item.get("category", ""),
            "related": item.get("related", ""),
            "summary": item.get("summary", ""),
            "url": item.get("url", ""),
            "image": item.get("image", ""),
        }
        for item in data
    ]
    df = pd.DataFrame(rows)
    return _NEWS_OUTPUT.build_table_result(
        df,
        provenance=Provenance(source="finnhub_market_news", params={"category": params.category}),
        params={"category": params.category},
    )


# ---------------------------------------------------------------------------
# Calendars — Earnings and IPO
# ---------------------------------------------------------------------------


@connector(output=_EARNINGS_CAL_OUTPUT, tags=["equities", "calendars"])
async def finnhub_earnings_calendar(params: FinnhubEarningsCalendarParams, *, api_key: str) -> Result:
    """Fetch upcoming or recent earnings release dates for all (or one) stock.
    Returns per-report: date, fiscal year/quarter, release timing (bmo=before
    market open, amc=after market close), EPS estimate/actual, and revenue
    estimate/actual. Actuals are null for future events. Free tier is limited
    to near-future and recent-past dates — deep historical dates return empty.
    """
    http = _make_http(api_key)
    req: dict[str, Any] = {"from": params.from_date, "to": params.to_date}
    if params.symbol:
        req["symbol"] = params.symbol

    data = await _fh_fetch(http, path="/calendar/earnings", params=req, op_name="finnhub_earnings_calendar")

    calendar = data.get("earningsCalendar", []) if isinstance(data, dict) else []
    if not calendar:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No earnings events between {params.from_date} and {params.to_date}",
        )

    rows = [
        {
            "symbol": e.get("symbol", ""),
            "date": e.get("date"),
            "year": e.get("year"),
            "quarter": e.get("quarter"),
            "hour": e.get("hour", ""),
            "eps_estimate": e.get("epsEstimate"),
            "eps_actual": e.get("epsActual"),
            "revenue_estimate": e.get("revenueEstimate"),
            "revenue_actual": e.get("revenueActual"),
        }
        for e in calendar
        if e.get("symbol")
    ]
    if not rows:
        raise EmptyDataError(provider="finnhub", message="Empty earnings calendar after filtering")

    df = pd.DataFrame(rows)
    return _EARNINGS_CAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="finnhub_earnings_calendar",
            params={"from": params.from_date, "to": params.to_date},
        ),
        params={"from": params.from_date, "to": params.to_date},
    )


@connector(output=_IPO_CAL_OUTPUT, tags=["equities", "calendars"])
async def finnhub_ipo_calendar(params: FinnhubIpoCalendarParams, *, api_key: str) -> Result:
    """Fetch IPO calendar for a date range: company name, ticker, exchange,
    status (expected/priced/filed/withdrawn), IPO price, number of shares, and
    gross proceeds. Price is parsed from a string field — may be null when not
    yet priced. Symbol may be empty for pre-priced IPOs. Recent 3-month window
    has the most complete data on the free tier.
    """
    http = _make_http(api_key)
    data = await _fh_fetch(
        http,
        path="/calendar/ipo",
        params={"from": params.from_date, "to": params.to_date},
        op_name="finnhub_ipo_calendar",
    )

    calendar = data.get("ipoCalendar", []) if isinstance(data, dict) else []
    if not calendar:
        raise EmptyDataError(
            provider="finnhub",
            message=f"No IPO events between {params.from_date} and {params.to_date}",
        )

    rows = []
    for ipo in calendar:
        # price is a string field ("10.00") or null — parse defensively
        price_raw = ipo.get("price")
        try:
            price = float(price_raw) if price_raw else None
        except (ValueError, TypeError):
            price = None

        rows.append(
            {
                "symbol": ipo.get("symbol", ""),
                "name": ipo.get("name", ""),
                "date": ipo.get("date"),
                "exchange": ipo.get("exchange"),
                "status": ipo.get("status", ""),
                "price": price,
                "number_of_shares": ipo.get("numberOfShares"),
                "total_shares_value": ipo.get("totalSharesValue"),
            }
        )

    if not rows:
        raise EmptyDataError(provider="finnhub", message="Empty IPO calendar after parsing")

    df = pd.DataFrame(rows)
    return _IPO_CAL_OUTPUT.build_table_result(
        df,
        provenance=Provenance(
            source="finnhub_ipo_calendar",
            params={"from": params.from_date, "to": params.to_date},
        ),
        params={"from": params.from_date, "to": params.to_date},
    )


# ---------------------------------------------------------------------------
# Enumerator — full US symbol list for catalog indexing
# ---------------------------------------------------------------------------


@enumerator(output=_ENUMERATE_OUTPUT, tags=["equities"])
async def enumerate_finnhub(params: FinnhubEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate all symbols from Finnhub for catalog indexing.

    Calls /stock/symbol — returns ~30 000 rows for exchange='US' with symbol,
    description (company name), type, currency, and MIC. The endpoint is served
    from a CDN static file snapshot so it has no rate-limit headers; refresh
    at most once per day.
    """
    http = HttpClient(
        _BASE_URL,
        headers={"X-Finnhub-Token": api_key},
        timeout=60.0,
        follow_redirects=True,
    )
    resp = await http.request("GET", "/stock/symbol", params={"exchange": params.exchange})
    resp.raise_for_status()
    data: list[dict] = resp.json()

    if not data:
        return pd.DataFrame(
            columns=["symbol", "description", "display_symbol", "type", "currency", "mic", "exchange", "isin"]
        )

    rows = [
        {
            "symbol": s.get("symbol", ""),
            "description": s.get("description", ""),
            "display_symbol": s.get("displaySymbol", ""),
            "type": s.get("type", ""),
            "currency": s.get("currency", ""),
            "mic": s.get("mic", ""),
            "exchange": params.exchange,
            "isin": s.get("isin", ""),
        }
        for s in data
        if s.get("symbol")
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Collection — kept as a literal ``Connectors([...])`` assignment so
# ``tools/gen_registry.py`` can AST-extract it.
# ---------------------------------------------------------------------------

CONNECTORS = Connectors(
    [
        # Discovery
        finnhub_search,
        # Fetch
        finnhub_quote,
        finnhub_profile,
        finnhub_peers,
        finnhub_recommendation,
        finnhub_earnings,
        finnhub_basic_financials,
        finnhub_company_news,
        finnhub_market_news,
        finnhub_earnings_calendar,
        finnhub_ipo_calendar,
        # Enumeration
        enumerate_finnhub,
    ]
)


__all__ = [
    "CONNECTORS",
    "ENV_VARS",
    # Parameter models (public — downstream callers type against these)
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
    # Connector functions
    "finnhub_basic_financials",
    "finnhub_company_news",
    "finnhub_earnings",
    "finnhub_earnings_calendar",
    "finnhub_ipo_calendar",
    "finnhub_market_news",
    "finnhub_peers",
    "finnhub_profile",
    "finnhub_quote",
    "finnhub_recommendation",
    "finnhub_search",
    # Enumerator
    "enumerate_finnhub",
]
