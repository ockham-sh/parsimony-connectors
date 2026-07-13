"""Tiingo source: equities, crypto, forex — EOD, real-time, and reference data.

API docs: https://www.tiingo.com/documentation/general/overview
Authentication: ``Authorization: Token <key>`` request header.
Base URL: https://api.tiingo.com
Rate limit: free tier ~50 req/hour, 500 req/day. No rate-limit headers.

Provides 13 connectors:

* Discovery: ``tiingo_search`` (tool-tagged for agent use).
* Equities: ``tiingo_eod``, ``tiingo_iex``, ``tiingo_iex_historical``,
  ``tiingo_meta``.
* Fundamentals (reference): ``tiingo_fundamentals_meta``,
  ``tiingo_fundamentals_definitions``.
* News: ``tiingo_news`` (Power+ plan — free tier returns 403, mapped to
  :class:`PaymentRequiredError`).
* Crypto: ``tiingo_crypto_prices``, ``tiingo_crypto_top``.
* Forex: ``tiingo_fx_prices``, ``tiingo_fx_top``.
* Enumerator: ``enumerate_tiingo`` (supported-tickers CDN snapshot for
  catalog indexing).

The API key is declared as a secret (stripped from provenance) and supplied
either by binding (``load(api_key=...)`` / ``Connector.bind``) or, as a dev
fallback, from the ``TIINGO_API_KEY`` environment variable. A missing key
fails fast with :class:`UnauthorizedError` naming the env var.

Internal layout (not part of the public contract):

* :mod:`parsimony_tiingo.outputs` — declarative :class:`OutputSpec` schemas.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    PaymentRequiredError,
)
from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import fetch_json, make_http_client, require_key

from parsimony_tiingo.outputs import (
    CRYPTO_PRICES_OUTPUT,
    CRYPTO_TOP_OUTPUT,
    DEFINITIONS_OUTPUT,
    ENUMERATE_OUTPUT,
    EOD_OUTPUT,
    FUNDAMENTALS_META_OUTPUT,
    FX_PRICES_OUTPUT,
    FX_TOP_OUTPUT,
    IEX_HIST_OUTPUT,
    IEX_OUTPUT,
    META_OUTPUT,
    NEWS_OUTPUT,
    SEARCH_OUTPUT,
)

__all__ = ["CONNECTORS", "load"]

_BASE_URL = "https://api.tiingo.com"
_ENV_VAR = "TIINGO_API_KEY"
_PROVIDER = "tiingo"
_TIMEOUT = 15.0

# Static CDN snapshot of every supported ticker. Large (~127k rows); refreshed
# at most once per day upstream. Downloaded as a zip → CSV by the enumerator.
# Served from a separate host (apimedia.tiingo.com), so the enumerator builds a
# dedicated client pointed at this base rather than reusing the API client.
_TICKERS_CDN_BASE = "https://apimedia.tiingo.com"
_TICKERS_ZIP_PATH = "docs/tiingo/daily/supported_tickers.zip"
_TICKERS_TIMEOUT = 120.0

# Regex guard for values interpolated directly into request paths
# (``/tiingo/daily/<ticker>/prices`` etc.). Anything outside the allowed
# character set is rejected before the URL is built.
_TICKER_RE = re.compile(r"^[a-zA-Z0-9._\-]+$")


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _client(api_key: str) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the Tiingo client.

    Tiingo auth is an ``Authorization: Token <key>`` header (not a query
    param), so the key never appears in a request URL / log line. A missing
    key raises :class:`UnauthorizedError` before any network call.
    """
    key = require_key(api_key, env_var=_ENV_VAR, provider=_PROVIDER)
    return make_http_client(
        _BASE_URL, provider=_PROVIDER, headers={"Authorization": f"Token {key}"}, timeout=_TIMEOUT
    )


def _safe_ticker(ticker: str) -> str:
    """Validate a path-interpolated ticker; raise InvalidParameterError if unsafe."""
    t = ticker.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "ticker must be non-empty")
    if not _TICKER_RE.match(t):
        raise InvalidParameterError(_PROVIDER, f"ticker contains unsafe characters for URL path: {ticker!r}")
    return t


# ---------------------------------------------------------------------------
# Discovery — search
# ---------------------------------------------------------------------------


@connector(output=SEARCH_OUTPUT, tags=["equities", "tool"], secrets=("api_key",))
def tiingo_search(query: str, limit: int = 25, api_key: str = "") -> pd.DataFrame:
    """Search Tiingo for stocks, ETFs, mutual funds, and crypto by name or ticker.

    Returns ticker (the stable API identifier), name, asset_type (Stock, ETF,
    Mutual Fund), is_active, and country_code. Use the ticker with tiingo_eod,
    tiingo_iex, tiingo_meta, or tiingo_fundamentals_meta for further data.
    Example: query='apple' → ticker='AAPL'; query='bitcoin' → ticker='BTCUSD'.

    Matching is a substring match on Tiingo's side, so a full punctuated legal name can
    miss: 'coca cola' does not match 'Coca-Cola', and multi-word queries like
    'bank america' return nothing. Search one distinctive token ('coca', 'nvidia') or a
    known ticker, not the whole company name.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError(_PROVIDER, "query must be non-empty")
    if limit < 1 or limit > 100:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 100")

    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/utilities/search",
        params={"query": q, "limit": limit},
        op_name="tiingo_search",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "search response was not a JSON array")
    rows = [
        {
            "ticker": r.get("ticker", ""),
            "name": r.get("name", ""),
            "asset_type": r.get("assetType", ""),
            "is_active": r.get("isActive", False),
            "country_code": r.get("countryCode", ""),
            "perma_ticker": r.get("permaTicker", ""),
            "open_figi": r.get("openFIGIComposite", ""),
        }
        for r in data
        if r.get("ticker")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"query": q})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Equities — EOD prices
# ---------------------------------------------------------------------------


@connector(output=EOD_OUTPUT, tags=["equities"], secrets=("api_key",))
def tiingo_eod(
    ticker: Annotated[str, Namespace("tiingo_ticker")],
    start_date: str | None = None,
    end_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch historical end-of-day OHLCV prices for a stock with split/dividend
    adjusted values. Returns date, open/high/low/close, volume, and adjusted
    counterparts (adj_open/high/low/close/volume), plus div_cash and
    split_factor. Free tier provides full history back to listing date.
    Use tiingo_search to resolve ticker symbols first.
    """
    t = _safe_ticker(ticker)
    req: dict[str, Any] = {"startDate": start_date, "endDate": end_date}

    http = _client(api_key)
    data = fetch_json(
        http,
        path=f"tiingo/daily/{t}/prices",
        params=req,
        op_name="tiingo_eod",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "EOD response was not a JSON array")
    if not data:
        raise EmptyDataError(_PROVIDER, query_params={"ticker": t})

    rows = [
        {
            "date": r.get("date"),
            "close": r.get("close"),
            "high": r.get("high"),
            "low": r.get("low"),
            "open": r.get("open"),
            "volume": r.get("volume"),
            "adj_close": r.get("adjClose"),
            "adj_high": r.get("adjHigh"),
            "adj_low": r.get("adjLow"),
            "adj_open": r.get("adjOpen"),
            "adj_volume": r.get("adjVolume"),
            "div_cash": r.get("divCash"),
            "split_factor": r.get("splitFactor"),
        }
        for r in data
    ]
    df = pd.DataFrame(rows)
    df["ticker"] = t
    return df


# ---------------------------------------------------------------------------
# Equities — IEX real-time quotes
# ---------------------------------------------------------------------------


@connector(output=IEX_OUTPUT, tags=["equities"], secrets=("api_key",))
def tiingo_iex(tickers: str, api_key: str = "") -> pd.DataFrame:
    """Fetch real-time IEX quotes for one or more stocks. Returns Tiingo's
    composite last price (tngo_last), OHLV for the day, previous close,
    mid/bid/ask prices and sizes. Timestamp is ISO 8601 UTC. Free tier
    supported. Comma-separate tickers; use tiingo_search to resolve them.
    """
    t = tickers.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "tickers must be non-empty")

    http = _client(api_key)
    data = fetch_json(
        http,
        path="iex/",
        params={"tickers": t},
        op_name="tiingo_iex",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "IEX response was not a JSON array")
    rows = [
        {
            "ticker": r.get("ticker", ""),
            "timestamp": r.get("timestamp"),
            "tngo_last": r.get("tngoLast"),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "volume": r.get("volume"),
            "prev_close": r.get("prevClose"),
            "mid": r.get("mid"),
            "bid_price": r.get("bidPrice"),
            "ask_price": r.get("askPrice"),
            "bid_size": r.get("bidSize"),
            "ask_size": r.get("askSize"),
        }
        for r in data
        if r.get("ticker")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Equities — IEX historical intraday
# ---------------------------------------------------------------------------


@connector(output=IEX_HIST_OUTPUT, tags=["equities"], secrets=("api_key",))
def tiingo_iex_historical(
    ticker: Annotated[str, Namespace("tiingo_ticker")],
    start_date: str | None = None,
    end_date: str | None = None,
    resample_freq: str = "1hour",
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch historical IEX intraday OHLC prices for a stock at a given
    frequency (1min/5min/15min/30min/1hour/2hour/4hour). Returns the most
    recent ~2000 data points at the specified frequency — cannot request
    arbitrarily old data. Free tier supported. Use tiingo_search first.

    The ``date`` column is tz-aware UTC (Tiingo emits ISO-8601 ``…Z`` timestamps,
    preserved faithfully) — intraday bars are timezone-meaningful, so it is not
    coerced to a naive date.
    """
    t = _safe_ticker(ticker)
    freq = resample_freq.strip()
    if not freq:
        raise InvalidParameterError(_PROVIDER, "resample_freq must be non-empty")
    req: dict[str, Any] = {"resampleFreq": freq, "startDate": start_date, "endDate": end_date}

    http = _client(api_key)
    data = fetch_json(
        http,
        path=f"iex/{t}/prices",
        params=req,
        op_name="tiingo_iex_historical",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "IEX intraday response was not a JSON array")
    if not data:
        raise EmptyDataError(_PROVIDER, query_params={"ticker": t, "resample_freq": freq})

    rows = [
        {
            "date": r.get("date"),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
        }
        for r in data
    ]
    df = pd.DataFrame(rows)
    df["ticker"] = t
    return df


# ---------------------------------------------------------------------------
# Equities — company metadata
# ---------------------------------------------------------------------------


@connector(output=META_OUTPUT, tags=["equities"], secrets=("api_key",))
def tiingo_meta(ticker: Annotated[str, Namespace("tiingo_ticker")], api_key: str = "") -> pd.DataFrame:
    """Fetch company metadata for a stock: ticker, name, description, exchange
    code, and listing start/end dates. Returns a one-row DataFrame. Use
    tiingo_search to resolve ticker symbols. For sector/industry data use
    tiingo_fundamentals_meta.
    """
    t = _safe_ticker(ticker)

    http = _client(api_key)
    data = fetch_json(
        http,
        path=f"tiingo/daily/{t}",
        op_name="tiingo_meta",
    )

    if not isinstance(data, dict):
        raise ParseError(_PROVIDER, "metadata response was not a JSON object")
    if not data.get("ticker"):
        raise EmptyDataError(_PROVIDER, query_params={"ticker": t})
    # Conform to the declared schema — the endpoint omits optional fields for
    # some tickers, so absent columns are materialised as NA (the schema is a
    # contract, and the strict column check would otherwise crash).
    return pd.DataFrame([data]).reindex(columns=[c.name for c in META_OUTPUT.columns])


# ---------------------------------------------------------------------------
# Fundamentals — company metadata (sector, industry, SIC)
# ---------------------------------------------------------------------------


@connector(output=FUNDAMENTALS_META_OUTPUT, tags=["equities"], secrets=("api_key",))
def tiingo_fundamentals_meta(tickers: str, api_key: str = "") -> pd.DataFrame:
    """Fetch fundamentals metadata for one or more stocks: sector, industry,
    SIC code/sector/industry, reporting currency, location, company website,
    SEC filing link, ADR flag, and data freshness timestamps. Returns one row
    per ticker. Comma-separate tickers; use tiingo_search to resolve them.
    """
    t = tickers.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "tickers must be non-empty")

    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/fundamentals/meta",
        params={"tickers": t},
        op_name="tiingo_fundamentals_meta",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "fundamentals meta response was not a JSON array")
    if not data:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})
    # One row per ticker, conformed to the declared schema (absent optional
    # fields → NA; the schema is a contract, strict column check enforces it).
    return pd.DataFrame(data).reindex(columns=[c.name for c in FUNDAMENTALS_META_OUTPUT.columns])


# ---------------------------------------------------------------------------
# Fundamentals — definitions (reference data)
# ---------------------------------------------------------------------------


@connector(output=DEFINITIONS_OUTPUT, tags=["equities"], secrets=("api_key",))
def tiingo_fundamentals_definitions(api_key: str = "") -> pd.DataFrame:
    """List all available fundamental metric definitions: data_code (metric ID),
    name, description, statement_type (overview/incomeStatement/balanceSheet/
    cashFlow), and units. Use data_codes to interpret fundamentals responses.
    Free tier supported.
    """
    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/fundamentals/definitions",
        op_name="tiingo_fundamentals_definitions",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "definitions response was not a JSON array")
    rows = [
        {
            "data_code": r.get("dataCode", ""),
            "name": r.get("name", ""),
            "description": r.get("description", ""),
            "statement_type": r.get("statementType", ""),
            "units": r.get("units", ""),
        }
        for r in data
        if r.get("dataCode")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# News (Power+ plan required)
# ---------------------------------------------------------------------------


def _fetch_news(http: HttpClient, params: dict[str, Any]) -> Any:
    """News GET with a custom 403 → PaymentRequiredError mapping.

    The News API is plan-gated (Power+). Tiingo returns **403** for THREE
    distinct conditions, distinguishable only by the body:
      * plan restriction  → "You do not have permission to access the News API"
      * invalid credential → "Invalid token."
      * absent credential  → "Please supply a token" (already fast-failed in _client)
    So only a *plan-restriction* body maps to :class:`PaymentRequiredError`; an
    invalid credential must fall through to the canonical mapper → ``UnauthorizedError``
    (a status-only 403→Payment mapping would mis-diagnose a typo'd/revoked key).
    """
    clean = {k: v for k, v in params.items() if v is not None}
    response = http.request("GET", "tiingo/news", params=clean or None, op_name="tiingo_news")
    if response.status_code == 403:
        body = (response.text or "").lower()
        if "permission" in body or "news api" in body:
            raise PaymentRequiredError(_PROVIDER)
    check_status(response, provider=_PROVIDER, op_name="tiingo_news")
    return response.json()


@connector(output=NEWS_OUTPUT, tags=["equities", "news"], secrets=("api_key",))
def tiingo_news(
    tickers: str | None = None,
    source: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 50,
    api_key: str = "",
) -> pd.DataFrame:
    """[Power+] Fetch news articles from Tiingo. Filter by tickers, source, and
    date range. Returns title, published_date, source, related tickers, tags,
    description, and url. Requires the Power+ plan — a free-tier key returns
    403, surfaced as PaymentRequiredError.
    """
    if limit < 1 or limit > 100:
        raise InvalidParameterError(_PROVIDER, "limit must be between 1 and 100")
    req: dict[str, Any] = {
        "limit": limit,
        "tickers": tickers,
        "source": source,
        "startDate": start_date,
        "endDate": end_date,
    }

    http = _client(api_key)
    data = _fetch_news(http, req)

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "news response was not a JSON array")
    if not data:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": tickers or ""})

    rows = [
        {
            "id": article.get("id"),
            "title": article.get("title", ""),
            "published_date": article.get("publishedDate"),
            "source": article.get("source", ""),
            "tickers": ",".join(article.get("tickers", [])) if article.get("tickers") else "",
            "tags": ",".join(article.get("tags", [])) if article.get("tags") else "",
            "description": article.get("description", ""),
            "url": article.get("url", ""),
        }
        for article in data
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Crypto — historical prices
# ---------------------------------------------------------------------------


@connector(output=CRYPTO_PRICES_OUTPUT, tags=["crypto"], secrets=("api_key",))
def tiingo_crypto_prices(
    tickers: str,
    start_date: str | None = None,
    end_date: str | None = None,
    resample_freq: str = "1day",
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch historical crypto OHLCV prices. Returns date, open, high, low,
    close, volume (in base currency), volume_notional (in quote currency), and
    trades_done. Supports resample frequencies from 1min to 1day. Free tier
    supported. Pairs are lowercase, e.g. 'btcusd', 'ethusd'.
    """
    t = tickers.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "tickers must be non-empty")
    freq = resample_freq.strip()
    if not freq:
        raise InvalidParameterError(_PROVIDER, "resample_freq must be non-empty")
    req: dict[str, Any] = {
        "tickers": t,
        "resampleFreq": freq,
        "startDate": start_date,
        "endDate": end_date,
    }

    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/crypto/prices",
        params=req,
        op_name="tiingo_crypto_prices",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})

    # Response: [{ticker, baseCurrency, quoteCurrency, priceData: [...]}]
    all_rows: list[dict[str, Any]] = []
    for entry in data:
        ticker = entry.get("ticker", t)
        for p in entry.get("priceData", []):
            all_rows.append(
                {
                    "ticker": ticker,
                    "date": p.get("date"),
                    "open": p.get("open"),
                    "high": p.get("high"),
                    "low": p.get("low"),
                    "close": p.get("close"),
                    "volume": p.get("volume"),
                    "volume_notional": p.get("volumeNotional"),
                    "trades_done": p.get("tradesDone"),
                }
            )

    if not all_rows:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})
    return pd.DataFrame(all_rows)


# ---------------------------------------------------------------------------
# Crypto — real-time top-of-book
# ---------------------------------------------------------------------------


@connector(output=CRYPTO_TOP_OUTPUT, tags=["crypto"], secrets=("api_key",))
def tiingo_crypto_top(tickers: str, api_key: str = "") -> pd.DataFrame:
    """Fetch real-time top-of-book quotes for crypto pairs: last price, bid/ask
    prices and sizes, last trade size (notional), and exchange. Free tier
    supported. Comma-separate pairs; use lowercase, e.g. 'btcusd', 'ethusd'.
    """
    t = tickers.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "tickers must be non-empty")

    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/crypto/top",
        params={"tickers": t},
        op_name="tiingo_crypto_top",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})

    # Response: [{ticker, baseCurrency, quoteCurrency, topOfBookData: [{...}]}]
    rows: list[dict[str, Any]] = []
    for entry in data:
        ticker = entry.get("ticker", "")
        for book in entry.get("topOfBookData", []):
            rows.append(
                {
                    "ticker": ticker,
                    "last_price": book.get("lastPrice"),
                    "quote_timestamp": book.get("quoteTimestamp"),
                    "bid_price": book.get("bidPrice"),
                    "ask_price": book.get("askPrice"),
                    "bid_size": book.get("bidSize"),
                    "ask_size": book.get("askSize"),
                    "last_size_notional": book.get("lastSizeNotional"),
                    "last_exchange": book.get("lastExchange", ""),
                }
            )

    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Forex — historical prices
# ---------------------------------------------------------------------------


@connector(output=FX_PRICES_OUTPUT, tags=["forex"], secrets=("api_key",))
def tiingo_fx_prices(
    tickers: str,
    start_date: str | None = None,
    end_date: str | None = None,
    resample_freq: str = "1day",
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch historical forex OHLC prices. Returns date, open, high, low, close
    for a currency pair. Supports resample frequencies from 1min to 1day. Free
    tier supported. Pairs are lowercase, e.g. 'eurusd', 'gbpjpy'.
    """
    t = tickers.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "tickers must be non-empty")
    freq = resample_freq.strip()
    if not freq:
        raise InvalidParameterError(_PROVIDER, "resample_freq must be non-empty")
    req: dict[str, Any] = {
        "tickers": t,
        "resampleFreq": freq,
        "startDate": start_date,
        "endDate": end_date,
    }

    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/fx/prices",
        params=req,
        op_name="tiingo_fx_prices",
    )

    if not isinstance(data, list) or not data:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})

    rows = [
        {
            "ticker": r.get("ticker", t),
            "date": r.get("date"),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
        }
        for r in data
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Forex — real-time top-of-book
# ---------------------------------------------------------------------------


@connector(output=FX_TOP_OUTPUT, tags=["forex"], secrets=("api_key",))
def tiingo_fx_top(tickers: str, api_key: str = "") -> pd.DataFrame:
    """Fetch real-time top-of-book forex quotes: mid, bid/ask prices and sizes.
    Free tier supported. Comma-separate pairs; use lowercase, e.g. 'eurusd',
    'gbpjpy'.
    """
    t = tickers.strip()
    if not t:
        raise InvalidParameterError(_PROVIDER, "tickers must be non-empty")

    http = _client(api_key)
    data = fetch_json(
        http,
        path="tiingo/fx/top",
        params={"tickers": t},
        op_name="tiingo_fx_top",
    )

    if not isinstance(data, list):
        raise ParseError(_PROVIDER, "forex top response was not a JSON array")
    rows = [
        {
            "ticker": r.get("ticker", ""),
            "quote_timestamp": r.get("quoteTimestamp"),
            "mid_price": r.get("midPrice"),
            "bid_price": r.get("bidPrice"),
            "ask_price": r.get("askPrice"),
            "bid_size": r.get("bidSize"),
            "ask_size": r.get("askSize"),
        }
        for r in data
        if r.get("ticker")
    ]
    if not rows:
        raise EmptyDataError(_PROVIDER, query_params={"tickers": t})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Enumerator — supported tickers for catalog indexing
# ---------------------------------------------------------------------------


def _download_supported_tickers(api_key: str) -> bytes:
    """Download the supported-tickers zip via the package transport.

    Routed through ``HttpClient`` (not a bare ``httpx.Client``) so CDN
    failures map to typed kernel errors and the same redaction policy applies.
    The zip is served from a separate public CDN host, so a dedicated client
    (longer timeout, no auth header) is built for it. ``_client`` is still
    called first so the enumerator's no-key path fast-fails with
    :class:`UnauthorizedError` like every other keyed verb.
    """
    _client(api_key)  # enforce the symmetric no-key fast-fail
    cdn = make_http_client(_TICKERS_CDN_BASE, provider=_PROVIDER, timeout=_TICKERS_TIMEOUT)
    response = cdn.request("GET", _TICKERS_ZIP_PATH, op_name="enumerate_tiingo")
    check_status(response, provider=_PROVIDER, op_name="enumerate_tiingo")
    return response.content


def _parse_supported_tickers(content: bytes) -> pd.DataFrame:
    """Parse the supported-tickers zip into the enumerator's declared schema.

    The CSV header is ``ticker, exchange, assetType, priceCurrency, startDate,
    endDate`` — there is **no** name or country column, so ``name`` (TITLE)
    falls back to the ticker symbol itself (the only human-facing label this
    snapshot carries). A non-zip / non-CSV payload raises ParseError.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
        csv_name = zf.namelist()[0]
        text = zf.read(csv_name).decode("utf-8")
    except (zipfile.BadZipFile, IndexError, UnicodeDecodeError) as exc:
        raise ParseError(_PROVIDER, f"supported_tickers payload was not a readable zip/CSV: {exc}") from exc

    reader = csv.DictReader(io.StringIO(text))
    rows = [
        {
            "ticker": row.get("ticker", ""),
            "name": row.get("ticker", ""),  # CSV has no name column; ticker is the only label
            "asset_type": row.get("assetType", ""),
            "exchange": row.get("exchange", ""),
            "price_currency": row.get("priceCurrency", ""),
            "start_date": row.get("startDate", ""),
            "end_date": row.get("endDate", ""),
        }
        for row in reader
        if row.get("ticker")
    ]
    if not rows:
        raise ParseError(_PROVIDER, "supported_tickers CSV contained no ticker rows")
    return pd.DataFrame(rows)


@enumerator(output=ENUMERATE_OUTPUT, tags=["equities"], secrets=("api_key",))
def enumerate_tiingo(api_key: str = "") -> pd.DataFrame:
    """Enumerate all supported tickers from Tiingo for catalog indexing.

    Downloads the supported_tickers.zip CSV from apimedia.tiingo.com — ~127 000
    rows with ticker, asset type, exchange, price currency, and start/end dates.
    The file is a static CDN snapshot; refresh at most once per day.
    """
    content = _download_supported_tickers(api_key)
    return _parse_supported_tickers(content)


# ---------------------------------------------------------------------------
# Connector collection
# ---------------------------------------------------------------------------

CONNECTORS = Connectors(
    [
        tiingo_search,
        tiingo_eod,
        tiingo_iex,
        tiingo_iex_historical,
        tiingo_meta,
        tiingo_fundamentals_meta,
        tiingo_fundamentals_definitions,
        tiingo_news,
        tiingo_crypto_prices,
        tiingo_crypto_top,
        tiingo_fx_prices,
        tiingo_fx_top,
        enumerate_tiingo,
    ]
)


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
