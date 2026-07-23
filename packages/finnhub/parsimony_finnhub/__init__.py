"""Finnhub source: equity quotes, company fundamentals, news, and calendars.

API docs: https://finnhub.io/docs/api
Authentication: ``token`` query parameter (in the transport sensitive-param
  set, so it is redacted from every log line).
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

The API key is declared as a secret (stripped from provenance) and supplied
either by binding (``load(api_key=...)`` / ``Connector.bind``) or, as a dev
fallback, from the ``FINNHUB_API_KEY`` environment variable. A missing key
fails fast with :class:`UnauthorizedError` naming the env var.

Status semantics differ from the canonical table on one point: an invalid key
returns 401 (→ :class:`UnauthorizedError`) while a premium-only endpoint on a
free plan returns 403 (→ :class:`PaymentRequiredError`). See
:mod:`parsimony_finnhub._http`.

Internal layout (not part of the public contract):

* :mod:`parsimony_finnhub._http` — keyed client builder and error mapping.
* :mod:`parsimony_finnhub.outputs` — declarative :class:`OutputSpec` schemas.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_finnhub._http import _client, finnhub_get
from parsimony_finnhub.outputs import EARNINGS_CAL_OUTPUT as _EARNINGS_CAL_OUTPUT
from parsimony_finnhub.outputs import EARNINGS_OUTPUT as _EARNINGS_OUTPUT
from parsimony_finnhub.outputs import ENUMERATE_OUTPUT as _ENUMERATE_OUTPUT
from parsimony_finnhub.outputs import IPO_CAL_OUTPUT as _IPO_CAL_OUTPUT
from parsimony_finnhub.outputs import NEWS_OUTPUT as _NEWS_OUTPUT
from parsimony_finnhub.outputs import PEERS_OUTPUT as _PEERS_OUTPUT
from parsimony_finnhub.outputs import PROFILE_OUTPUT as _PROFILE_OUTPUT
from parsimony_finnhub.outputs import QUOTE_OUTPUT as _QUOTE_OUTPUT
from parsimony_finnhub.outputs import RECOMMENDATION_OUTPUT as _RECOMMENDATION_OUTPUT
from parsimony_finnhub.outputs import SEARCH_OUTPUT as _SEARCH_OUTPUT

__all__ = ["CONNECTORS", "load"]

_PROVIDER = "finnhub"

# Enumerator timeout: /stock/symbol 302-redirects to a multi-MB CDN file.
_ENUMERATE_TIMEOUT = 60.0


def _require(value: str, name: str) -> str:
    """Strip and require a non-empty scalar string argument."""
    cleaned = value.strip()
    if not cleaned:
        raise InvalidParameterError(_PROVIDER, f"{name} must be non-empty")
    return cleaned


# ---------------------------------------------------------------------------
# Discovery — Symbol Search
# ---------------------------------------------------------------------------


@connector(output=_SEARCH_OUTPUT, tags=["equities", "tool"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_search(query: str, api_key: str = "") -> pd.DataFrame:
    """Search Finnhub for stocks, ETFs, and indices by name or ticker symbol.
    Returns symbol (the stable API identifier), description (company name),
    display_symbol, and type. Use symbol with finnhub_quote, finnhub_profile,
    or finnhub_company_news. Returns US and international symbols.

    Example: query='apple' → symbol='AAPL'; query='tesla' → symbol='TSLA'.
    """
    q = _require(query, "query")
    http = _client(api_key)
    data = finnhub_get(http, path="/search", params={"q": q}, op_name="finnhub_search")

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "search response was not a JSON object")
    results = data.get("result", [])
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
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"query": q})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Market Data — Real-time Quote
# ---------------------------------------------------------------------------


@connector(output=_QUOTE_OUTPUT, tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_quote(symbol: Annotated[str, Namespace("finnhub_symbol")], api_key: str = "") -> pd.DataFrame:
    """Fetch real-time quote for a stock: current price, day high/low/open,
    previous close, and absolute/percent change vs prior close. Timestamp
    is day-granularity (last close time, not a tick timestamp). Use
    finnhub_search to resolve ticker symbols first.
    """
    s = _require(symbol, "symbol")
    http = _client(api_key)
    data = finnhub_get(http, path="/quote", params={"symbol": s}, op_name="finnhub_quote")

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "quote response was not a JSON object")
    if data.get("c") is None:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s})

    row = {
        "symbol": s,
        "current_price": data.get("c"),
        "change": data.get("d"),
        "change_percent": data.get("dp"),
        "high": data.get("h"),
        "low": data.get("l"),
        "open": data.get("o"),
        "prev_close": data.get("pc"),
        "timestamp": data.get("t"),
    }
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# Company — Profile, Peers, Recommendations, Earnings, Fundamentals
# ---------------------------------------------------------------------------


@connector(output=_PROFILE_OUTPUT, tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_profile(symbol: Annotated[str, Namespace("finnhub_symbol")], api_key: str = "") -> pd.DataFrame:
    """Fetch company profile for a stock: name, exchange, country, currency,
    IPO date, industry, market cap (in millions USD), shares outstanding
    (millions), website, phone, and logo URL. Returns a one-row DataFrame.
    Use finnhub_search to resolve ticker symbols. For time-series fundamentals
    use finnhub_basic_financials.
    """
    s = _require(symbol, "symbol")
    http = _client(api_key)
    data = finnhub_get(http, path="/stock/profile2", params={"symbol": s}, op_name="finnhub_profile")

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "profile response was not a JSON object")
    if not data.get("name"):
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s})
    # Conform to the declared schema — profile2 omits fields for some symbols,
    # so absent columns are materialised as NA (the schema is a contract, and
    # the strict column check would otherwise crash).
    return pd.DataFrame([data]).reindex(columns=[c.name for c in _PROFILE_OUTPUT.columns])


@connector(output=_PEERS_OUTPUT, tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_peers(symbol: Annotated[str, Namespace("finnhub_symbol")], api_key: str = "") -> pd.DataFrame:
    """Fetch peer/comparable companies for a stock. Returns a list of ticker
    symbols in the same industry and market cap range. Use finnhub_quote or
    finnhub_profile on returned symbols for further analysis.
    """
    s = _require(symbol, "symbol")
    http = _client(api_key)
    data = finnhub_get(http, path="/stock/peers", params={"symbol": s}, op_name="finnhub_peers")

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "peers response was not a JSON array")
    peers = [p for p in data if isinstance(p, str) and p]
    if not peers:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s})
    return pd.DataFrame({"symbol": peers})


@connector(output=_RECOMMENDATION_OUTPUT, tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_recommendation(symbol: Annotated[str, Namespace("finnhub_symbol")], api_key: str = "") -> pd.DataFrame:
    """Fetch analyst buy/hold/sell recommendation trends for a stock.
    Returns monthly aggregated counts: strong_buy, buy, hold, sell, strong_sell.
    Free tier returns approximately the last 4 months of data.
    """
    s = _require(symbol, "symbol")
    http = _client(api_key)
    data = finnhub_get(
        http,
        path="/stock/recommendation",
        params={"symbol": s},
        op_name="finnhub_recommendation",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "recommendation response was not a JSON array")
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
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s})
    return pd.DataFrame(rows)


@connector(output=_EARNINGS_OUTPUT, tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_earnings(symbol: Annotated[str, Namespace("finnhub_symbol")], api_key: str = "") -> pd.DataFrame:
    """Fetch historical earnings per share (EPS) for a stock: actual EPS,
    consensus estimate, surprise, and surprise percent for the last ~4 quarters.
    For forward-looking earnings dates use finnhub_earnings_calendar.
    """
    s = _require(symbol, "symbol")
    http = _client(api_key)
    data = finnhub_get(http, path="/stock/earnings", params={"symbol": s}, op_name="finnhub_earnings")

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "earnings response was not a JSON array")
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
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s})
    return pd.DataFrame(rows)


@connector(tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_basic_financials(symbol: Annotated[str, Namespace("finnhub_symbol")], api_key: str = "") -> dict[str, Any]:
    """Fetch ~120 fundamental metrics for a stock: PE, EPS, beta, 52-week
    high/low, gross margin, ROE, dividend yield, market cap, and more.
    Also includes annual and quarterly time-series (going back to ~2007 for
    mature companies) for 37+ metrics: book value, EV/EBITDA, net margin, etc.
    Each time-series entry has period (ISO date) and v (float value).
    Response is a large dict with 'metric' (flat KPIs) and 'series' (time series);
    'series' splits into 'annual' and 'quarterly', each mapping a metric name to a
    list of those {period, v} entries.
    """
    s = _require(symbol, "symbol")
    http = _client(api_key)
    data = finnhub_get(
        http,
        path="/stock/metric",
        params={"symbol": s, "metric": "all"},
        op_name="finnhub_basic_financials",
    )

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "basic financials response was not a JSON object")
    if not data.get("metric"):
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s})
    return data


# ---------------------------------------------------------------------------
# News — Company and Market
# ---------------------------------------------------------------------------


@connector(output=_NEWS_OUTPUT, tags=["equities", "news"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_company_news(
    symbol: Annotated[str, Namespace("finnhub_symbol")],
    from_date: str,
    to_date: str,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch news articles for a specific company between two dates.
    Returns headline, source, publish datetime (unix timestamp), summary,
    URL, and related ticker. Free tier access is limited to recent months —
    historical dates silently return empty results (surfaced as EmptyDataError).
    Note: URL may be a Finnhub proxy redirect rather than a direct publisher link.
    """
    s = _require(symbol, "symbol")
    f = _require(from_date, "from_date")
    t = _require(to_date, "to_date")
    if f > t:
        raise InvalidParameterError(_PROVIDER, "from_date must not be after to_date")

    http = _client(api_key)
    data = finnhub_get(
        http,
        path="/company-news",
        params={"symbol": s, "from": f, "to": t},
        op_name="finnhub_company_news",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "company news response was not a JSON array")
    rows = _news_rows(data)
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"symbol": s, "from": f, "to": t})
    return pd.DataFrame(rows)


@connector(output=_NEWS_OUTPUT, tags=["news"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_market_news(
    category: Literal["general", "forex", "crypto", "merger"] = "general",
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch latest market-wide news by category. Categories: 'general' (top
    business/market headlines), 'forex', 'crypto', 'merger'. Returns up to
    ~100 articles. The 'related' field is empty for market news (unlike company
    news). For company-specific articles use finnhub_company_news.
    """
    http = _client(api_key)
    data = finnhub_get(http, path="/news", params={"category": category}, op_name="finnhub_market_news")

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "market news response was not a JSON array")
    rows = _news_rows(data)
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"category": category})
    return pd.DataFrame(rows)


def _news_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project the shared finnhub news article shape into NEWS_OUTPUT rows."""
    return [
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
        if item.get("id") is not None
    ]


# ---------------------------------------------------------------------------
# Calendars — Earnings and IPO
# ---------------------------------------------------------------------------


@connector(
    output=_EARNINGS_CAL_OUTPUT,
    tags=["equities", "calendars"],
    secrets=("api_key",),
    requires=("FINNHUB_API_KEY",),
)
def finnhub_earnings_calendar(
    from_date: str, to_date: str, symbol: str | None = None, api_key: str = ""
) -> pd.DataFrame:
    """Fetch upcoming or recent earnings release dates for all (or one) stock.
    Returns per-report: date, fiscal year/quarter, release timing (bmo=before
    market open, amc=after market close), EPS estimate/actual, and revenue
    estimate/actual. Actuals are null for future events. Free tier is limited
    to near-future and recent-past dates — deep historical dates return empty.
    """
    f = _require(from_date, "from_date")
    t = _require(to_date, "to_date")
    if f > t:
        raise InvalidParameterError(_PROVIDER, "from_date must not be after to_date")

    req: dict[str, Any] = {"from": f, "to": t}
    if symbol and symbol.strip():
        req["symbol"] = symbol.strip()

    http = _client(api_key)
    data = finnhub_get(http, path="/calendar/earnings", params=req, op_name="finnhub_earnings_calendar")

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "earnings calendar response was not a JSON object")
    calendar = data.get("earningsCalendar", [])
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
        raise EmptyDataError(_PROVIDER, query_params={"from": f, "to": t})
    return pd.DataFrame(rows)


@connector(output=_IPO_CAL_OUTPUT, tags=["equities", "calendars"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def finnhub_ipo_calendar(from_date: str, to_date: str, api_key: str = "") -> pd.DataFrame:
    """Fetch IPO calendar for a date range: company name, ticker, exchange,
    status (expected/priced/filed/withdrawn), price_range (the raw IPO price
    string — a single value or a range like '18.00-20.00'), number of shares,
    and gross proceeds. Symbol may be empty for pre-priced IPOs. Recent 3-month
    window has the most complete data on the free tier.
    """
    f = _require(from_date, "from_date")
    t = _require(to_date, "to_date")
    if f > t:
        raise InvalidParameterError(_PROVIDER, "from_date must not be after to_date")

    http = _client(api_key)
    data = finnhub_get(
        http,
        path="/calendar/ipo",
        params={"from": f, "to": t},
        op_name="finnhub_ipo_calendar",
    )

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "IPO calendar response was not a JSON object")
    calendar = data.get("ipoCalendar", [])
    rows = [
        {
            "symbol": ipo.get("symbol", ""),
            "name": ipo.get("name", ""),
            "date": ipo.get("date"),
            "exchange": ipo.get("exchange"),
            "status": ipo.get("status", ""),
            # Verbatim string ("18.00" or "18.00-20.00"); see IPO_CAL_OUTPUT.
            "price_range": ipo.get("price", ""),
            "number_of_shares": ipo.get("numberOfShares"),
            "total_shares_value": ipo.get("totalSharesValue"),
        }
        for ipo in calendar
        if ipo.get("name")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"from": f, "to": t})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Enumerator — full US symbol list for catalog indexing
# ---------------------------------------------------------------------------


@enumerator(output=_ENUMERATE_OUTPUT, tags=["equities"], secrets=("api_key",), requires=("FINNHUB_API_KEY",))
def enumerate_finnhub(exchange: str = "US", api_key: str = "") -> pd.DataFrame:
    """Enumerate all symbols from Finnhub for catalog indexing.

    Calls /stock/symbol — returns ~30 000 rows for exchange='US' with symbol,
    description (company name), type, currency, and MIC. The endpoint 302
    redirects to a CDN static-file snapshot (no rate-limit headers); refresh
    at most once per day.
    """
    ex = _require(exchange, "exchange")
    # Longer timeout + the canonical error mapping (the old path had none).
    http = _client(api_key, timeout=_ENUMERATE_TIMEOUT)
    data = finnhub_get(
        http,
        path="/stock/symbol",
        params={"exchange": ex},
        op_name="enumerate_finnhub",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "symbol list response was not a JSON array")
    rows = [
        {
            "symbol": s.get("symbol", ""),
            "description": s.get("description", ""),
            "display_symbol": s.get("displaySymbol", ""),
            "type": s.get("type", ""),
            "currency": s.get("currency", ""),
            "mic": s.get("mic", ""),
            "exchange": ex,
            "isin": s.get("isin", ""),
        }
        for s in data
        if s.get("symbol")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"exchange": ex})
    # Enumerators drop unmapped columns then require an EXACT match; select the
    # declared columns explicitly to keep the shape stable.
    return pd.DataFrame(rows)[[c.name for c in _ENUMERATE_OUTPUT.columns]]


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


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector."""
    return CONNECTORS.bind(api_key=api_key)
