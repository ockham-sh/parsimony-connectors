"""Polymarket connectors for parsimony.

Polymarket exposes two public read APIs with **no credentials**:

* the **Gamma** API (``https://gamma-api.polymarket.com``) lists markets and
  events;
* the **CLOB** API (``https://clob.polymarket.com``) serves order-book prices.

Because both APIs are keyless, this package declares no ``secrets=``, no
``api_key`` parameter, and no ``load(*, api_key=...)`` convenience — all GETs
go through :func:`parsimony.transport.helpers.fetch_json` over a plain
:func:`make_http_client` client.

Exports :data:`CONNECTORS`:

* ``polymarket_markets`` (``@enumerator``) — discover markets (id + question).
* ``polymarket_events`` (``@enumerator``) — discover events (id + title).
* ``polymarket_market_prices`` (``@connector``) — current CLOB buy-side price
  for one outcome token (a scalar lookup, not entity discovery).
"""

from __future__ import annotations

import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient
from parsimony.transport.helpers import fetch_json, make_http_client

__all__ = ["CONNECTORS"]

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_CLOB_BASE = "https://clob.polymarket.com"

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------
#
# markets/events are entity-discovery verbs (id + human title, no observation
# values) — the enumerator shape: exactly one namespaced KEY + >=1 TITLE +
# METADATA, no DATA. @enumerator enforces an EXACT column match, so the frame
# returned must contain exactly these columns (see §8.2).

_MARKETS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="polymarket_market"),
        Column(name="question", role=ColumnRole.TITLE),
        Column(name="slug", role=ColumnRole.METADATA),
        Column(name="active", role=ColumnRole.METADATA),
        Column(name="clobTokenIds", role=ColumnRole.METADATA),
    ]
)

_EVENTS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="polymarket_event"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="slug", role=ColumnRole.METADATA),
    ]
)

_MARKETS_COLUMNS = [c.name for c in _MARKETS_OUTPUT.columns]
_EVENTS_COLUMNS = [c.name for c in _EVENTS_OUTPUT.columns]


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
# Connectors
# ---------------------------------------------------------------------------


@enumerator(output=_MARKETS_OUTPUT, tags=["polymarket"])
def polymarket_markets(limit: int = 20, active: bool = True) -> pd.DataFrame:
    """Discover Polymarket markets via the Gamma API.

    Returns up to `limit` markets (1-100) as id + question (title) rows with
    slug and active-status metadata. `active=True` restricts to open markets.
    """
    if limit < 1 or limit > 100:
        raise InvalidParameterError("polymarket", "limit must be between 1 and 100")

    data = fetch_json(
        _gamma_http(),
        path="markets",
        params={"limit": limit, "active": str(active).lower()},
        op_name="markets",
    )

    if not isinstance(data, list):
        raise ParseError("polymarket", "Gamma /markets did not return a JSON array")
    if not data:
        raise EmptyDataError("polymarket", query_params={"limit": limit, "active": active})

    df = pd.DataFrame(data)
    missing = [c for c in _MARKETS_COLUMNS if c not in df.columns]
    if missing:
        raise ParseError("polymarket", f"Gamma /markets response missing fields: {missing}")
    # @enumerator requires an EXACT column match — return only declared columns.
    return df[_MARKETS_COLUMNS].head(limit)


@enumerator(output=_EVENTS_OUTPUT, tags=["polymarket"])
def polymarket_events(limit: int = 20) -> pd.DataFrame:
    """Discover Polymarket events via the Gamma API.

    Returns up to `limit` events (1-100) as id + title rows with slug metadata.
    An event groups one or more related markets.
    """
    if limit < 1 or limit > 100:
        raise InvalidParameterError("polymarket", "limit must be between 1 and 100")

    data = fetch_json(
        _gamma_http(),
        path="events",
        params={"limit": limit},
        op_name="events",
    )

    if not isinstance(data, list):
        raise ParseError("polymarket", "Gamma /events did not return a JSON array")
    if not data:
        raise EmptyDataError("polymarket", query_params={"limit": limit})

    df = pd.DataFrame(data)
    missing = [c for c in _EVENTS_COLUMNS if c not in df.columns]
    if missing:
        raise ParseError("polymarket", f"Gamma /events response missing fields: {missing}")
    # @enumerator requires an EXACT column match — return only declared columns.
    return df[_EVENTS_COLUMNS].head(limit)


@connector(tags=["polymarket", "tool"])
def polymarket_market_prices(token_id: str) -> dict[str, float]:
    """Fetch the current CLOB buy-side price for a Polymarket outcome token.

    `token_id` is a CLOB ERC-1155 token id (the `clobTokenIds` field on a
    Gamma market). Returns a single-key dict with the price as a float, e.g.
    ``{"price": 0.51}`` (the CLOB API sends it as a string; it is coerced here).
    A lone scalar is left as a dict rather than wrapped in a one-cell DataFrame.
    """
    token = token_id.strip()
    if not token:
        raise InvalidParameterError("polymarket", "token_id is required")

    data = fetch_json(
        _clob_http(),
        path="price",
        params={"token_id": token, "side": "buy"},
        op_name="price",
    )

    if not isinstance(data, dict) or "price" not in data:
        raise EmptyDataError(
            "polymarket",
            message=f"No price returned for token_id={token!r}",
            query_params={"token_id": token},
        )
    try:
        price = float(data["price"])
    except (TypeError, ValueError) as exc:
        raise ParseError("polymarket", f"price was not a number: {data['price']!r}") from exc
    return {"price": price}


CONNECTORS = Connectors([polymarket_markets, polymarket_events, polymarket_market_prices])
