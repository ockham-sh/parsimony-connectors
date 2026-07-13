"""Polymarket connectors for parsimony.

Polymarket exposes two public read APIs with **no credentials**:

* the **Gamma** API (``https://gamma-api.polymarket.com``) searches events and
  serves event/market structure;
* the **CLOB** API (``https://clob.polymarket.com``) serves order-book price
  history.

Because both APIs are keyless, this package declares no ``secrets=``, no
``api_key`` parameter, and no ``load(*, api_key=...)`` convenience — all GETs
go through :func:`parsimony.transport.helpers.fetch_json` over a plain
:func:`make_http_client` client.

The surface mirrors how a Polymarket question is actually navigated — search,
then drill from event to market to outcome token, then pull that token's price
history:

* ``polymarket_search_events`` (``@enumerator``) — natural-language search over
  events via Gamma ``/public-search`` (the only Polymarket endpoint that does
  server-side text search).
* ``polymarket_event`` (``@enumerator``) — the markets inside one event, by
  event slug.
* ``polymarket_market`` (``@enumerator``) — the outcomes of one market and their
  CLOB token ids, by market slug.
* ``polymarket_price_history`` (``@connector``) — the probability time series for
  one outcome token from the CLOB ``/prices-history`` endpoint.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import Column, ColumnRole, OutputSpec
from parsimony.transport import HttpClient
from parsimony.transport.helpers import fetch_json, make_http_client

__all__ = ["CONNECTORS"]

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_CLOB_BASE = "https://clob.polymarket.com"

# CLOB /prices-history accepts a fixed set of lookback windows.
_INTERVALS = ("max", "1m", "1w", "1d", "6h", "1h")

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------
#
# The three navigation verbs are entity-discovery (@enumerator): each returns a
# single namespaced KEY + >=1 TITLE + METADATA and no DATA columns. @enumerator
# enforces an EXACT column match, so every builder below emits exactly its
# declared columns. Market economics (volume/liquidity/active/closed) ride along
# as METADATA so the caller can triage which market is worth pulling — they are
# descriptive attributes of the entity, not observation values.

_SEARCH_EVENTS_OUTPUT = OutputSpec(
    columns=[
        Column(name="slug", role=ColumnRole.KEY, namespace="polymarket_event"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="markets_count", role=ColumnRole.METADATA),
        Column(name="volume", role=ColumnRole.METADATA),
        Column(name="liquidity", role=ColumnRole.METADATA),
        Column(name="active", role=ColumnRole.METADATA),
        Column(name="closed", role=ColumnRole.METADATA),
    ]
)

_EVENT_MARKETS_OUTPUT = OutputSpec(
    columns=[
        Column(name="market_slug", role=ColumnRole.KEY, namespace="polymarket_market"),
        Column(name="market_question", role=ColumnRole.TITLE),
        Column(name="market_description", role=ColumnRole.METADATA),
        Column(name="market_outcomes_count", role=ColumnRole.METADATA),
        Column(name="market_volume", role=ColumnRole.METADATA),
        Column(name="market_liquidity", role=ColumnRole.METADATA),
        Column(name="market_active", role=ColumnRole.METADATA),
        Column(name="market_closed", role=ColumnRole.METADATA),
    ]
)

_MARKET_OUTCOMES_OUTPUT = OutputSpec(
    columns=[
        Column(name="clob_token_id", role=ColumnRole.KEY, namespace="polymarket_token"),
        Column(name="outcome", role=ColumnRole.TITLE),
    ]
)

# price history IS observation data: one probability per timestamp for one token.
_PRICE_HISTORY_OUTPUT = OutputSpec(
    columns=[
        Column(name="token", role=ColumnRole.KEY, namespace="polymarket_token"),
        Column(name="timestamp", role=ColumnRole.DATA),
        Column(name="probability", role=ColumnRole.DATA),
    ]
)

_SEARCH_EVENTS_COLUMNS = [c.name for c in _SEARCH_EVENTS_OUTPUT.columns]
_EVENT_MARKETS_COLUMNS = [c.name for c in _EVENT_MARKETS_OUTPUT.columns]
_MARKET_OUTCOMES_COLUMNS = [c.name for c in _MARKET_OUTCOMES_OUTPUT.columns]


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _gamma_http() -> HttpClient:
    """Build a keyless Gamma HTTP client."""
    return make_http_client(_GAMMA_BASE, provider="polymarket")


def _clob_http() -> HttpClient:
    """Build a keyless CLOB HTTP client."""
    return make_http_client(_CLOB_BASE, provider="polymarket")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _num(value: Any) -> float | None:
    """Coerce a Gamma numeric field (often a string) to float, else None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_list(value: Any) -> list[Any]:
    """Parse a Gamma field that may be a JSON-encoded array (e.g. ``'["Yes","No"]'``)."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _event_row(event: dict[str, Any]) -> dict[str, Any]:
    """Project a Gamma event into the search-events schema."""
    return {
        "slug": event.get("slug"),
        "title": event.get("title"),
        "description": event.get("description", ""),
        "markets_count": len(event.get("markets") or []),
        "volume": _num(event.get("volume")),
        "liquidity": _num(event.get("liquidity")),
        "active": event.get("active"),
        "closed": event.get("closed"),
    }


def _market_row(market: dict[str, Any]) -> dict[str, Any]:
    """Project a Gamma market into the event-markets schema."""
    return {
        "market_slug": market.get("slug"),
        "market_question": market.get("question"),
        "market_description": market.get("description", ""),
        "market_outcomes_count": len(_json_list(market.get("outcomes"))),
        "market_volume": _num(market.get("volume")),
        "market_liquidity": _num(market.get("liquidity")),
        "market_active": market.get("active"),
        "market_closed": market.get("closed"),
    }


def _first_object(data: Any) -> dict[str, Any] | None:
    """Gamma slug endpoints return either a bare object or a one-element list."""
    if isinstance(data, list):
        return data[0] if data and isinstance(data[0], dict) else None
    return data if isinstance(data, dict) else None


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@enumerator(output=_SEARCH_EVENTS_OUTPUT, tags=["polymarket"])
def polymarket_search_events(
    search_text: str, limit: int = 20, optimized: bool = False
) -> pd.DataFrame:
    """Search Polymarket prediction market events by natural-language query.

    The entry point for finding the implied probability (odds) on a question —
    elections, politics, macro, crypto, sports. `search_text` is matched
    server-side against event titles/descriptions via Gamma ``/public-search``
    (the only Polymarket text-search endpoint; single concept words match better
    than long phrases).

    Returns up to `limit` events (1-100) as slug + title rows with description,
    market count, and volume/liquidity/active/closed metadata to triage which
    event to drill into with `polymarket_event`. `volume`/`liquidity` populate
    only when ``optimized=False`` (the default); ``optimized=True`` returns null.
    """
    query = search_text.strip()
    if not query:
        raise InvalidParameterError("polymarket", "search_text is required")
    if limit < 1 or limit > 100:
        raise InvalidParameterError("polymarket", "limit must be between 1 and 100")

    data = fetch_json(
        _gamma_http(),
        path="public-search",
        params={"q": query, "limit_per_type": limit, "optimized": str(optimized).lower()},
        op_name="public-search",
    )

    events = data.get("events") if isinstance(data, dict) else None
    if not isinstance(events, list):
        raise ParseError("polymarket", "Gamma /public-search did not return an 'events' array")
    if not events:
        raise EmptyDataError("polymarket", query_params={"q": query})

    rows = [_event_row(e) for e in events[:limit] if isinstance(e, dict)]
    return pd.DataFrame(rows, columns=_SEARCH_EVENTS_COLUMNS)


@enumerator(output=_EVENT_MARKETS_OUTPUT, tags=["polymarket"])
def polymarket_event(slug: str) -> pd.DataFrame:
    """List the markets inside one Polymarket prediction-market event, by slug.

    `slug` is an event slug (the `slug` field from `polymarket_search_events`).
    Returns one row per market — market slug + question, plus outcome count and
    volume/liquidity/active/closed metadata (populated here regardless of the
    search step's `optimized` flag). Feed a `market_slug` into
    `polymarket_market` to get its outcome tokens.
    """
    key = slug.strip()
    if not key:
        raise InvalidParameterError("polymarket", "slug is required")

    data = fetch_json(_gamma_http(), path=f"events/slug/{key}", op_name="event")
    event = _first_object(data)
    if event is None or "markets" not in event:
        raise ParseError("polymarket", f"Gamma /events/slug returned no event for slug={key!r}")

    markets = event.get("markets") or []
    if not markets:
        raise EmptyDataError(
            "polymarket", message=f"event {key!r} has no markets", query_params={"slug": key}
        )

    rows = [_market_row(m) for m in markets if isinstance(m, dict)]
    return pd.DataFrame(rows, columns=_EVENT_MARKETS_COLUMNS)


@enumerator(output=_MARKET_OUTCOMES_OUTPUT, tags=["polymarket"])
def polymarket_market(slug: str) -> pd.DataFrame:
    """List the outcomes of one Polymarket market and their CLOB token ids, by slug.

    `slug` is a market slug (the `market_slug` field from `polymarket_event`).
    Returns one row per outcome — the CLOB token id (key) and the outcome label
    (e.g. "Yes"/"No"). Feed a `clob_token_id` into `polymarket_price_history` to
    get that outcome's implied-probability (odds) time series.
    """
    key = slug.strip()
    if not key:
        raise InvalidParameterError("polymarket", "slug is required")

    data = fetch_json(_gamma_http(), path=f"markets/slug/{key}", op_name="market")
    market = _first_object(data)
    if market is None or "outcomes" not in market:
        raise ParseError("polymarket", f"Gamma /markets/slug returned no market for slug={key!r}")

    outcomes = _json_list(market.get("outcomes"))
    tokens = _json_list(market.get("clobTokenIds"))
    if not outcomes or not tokens:
        raise EmptyDataError(
            "polymarket",
            message=f"market {key!r} exposes no outcome tokens",
            query_params={"slug": key},
        )

    rows = [
        {"clob_token_id": tokens[i] if i < len(tokens) else None, "outcome": outcome}
        for i, outcome in enumerate(outcomes)
    ]
    return pd.DataFrame(rows, columns=_MARKET_OUTCOMES_COLUMNS)


@connector(output=_PRICE_HISTORY_OUTPUT, tags=["polymarket", "tool"])
def polymarket_price_history(
    token_id: str, interval: str = "1w", fidelity: int = 60
) -> pd.DataFrame:
    """Fetch the implied probability (odds) time series for one Polymarket token.

    `token_id` is a CLOB token id (`clob_token_id` from `polymarket_market`).
    `interval` is the lookback window (max/1m/1w/1d/6h/1h); `fidelity` is the
    sample resolution in minutes. Returns a tidy `timestamp` × `probability`
    frame.

    Two gotchas: the endpoint caps output at ~720 points, so `interval="max"`
    does NOT reach full history at a fine `fidelity` — for the true full history
    use a coarse fidelity (e.g. ``interval="max", fidelity=1440`` for daily). And
    a resolved/closed market has no order book, so this raises `EmptyDataError`
    rather than returning a frame.
    """
    token = token_id.strip()
    if not token:
        raise InvalidParameterError("polymarket", "token_id is required")
    if interval not in _INTERVALS:
        raise InvalidParameterError(
            "polymarket", f"interval must be one of {list(_INTERVALS)}; got {interval!r}"
        )
    if fidelity < 1:
        raise InvalidParameterError("polymarket", "fidelity must be a positive number of minutes")

    data = fetch_json(
        _clob_http(),
        path="prices-history",
        params={"market": token, "interval": interval, "fidelity": fidelity},
        op_name="prices-history",
    )

    history = data.get("history") if isinstance(data, dict) else None
    if not isinstance(history, list):
        raise ParseError("polymarket", "CLOB /prices-history did not return a 'history' array")
    if not history:
        raise EmptyDataError(
            "polymarket",
            message=f"no price history for token_id={token!r}",
            query_params={"market": token, "interval": interval},
        )

    raw = pd.DataFrame(history)
    if "t" not in raw.columns or "p" not in raw.columns:
        raise ParseError("polymarket", "CLOB /prices-history rows missing 't'/'p' fields")
    return pd.DataFrame(
        {
            "token": token,
            "timestamp": pd.to_datetime(raw["t"], unit="s", utc=True),
            "probability": pd.to_numeric(raw["p"], errors="coerce"),
        }
    )


CONNECTORS = Connectors(
    [
        polymarket_search_events,
        polymarket_event,
        polymarket_market,
        polymarket_price_history,
    ]
)
