"""Polymarket source: typed Gamma and CLOB connectors."""

from __future__ import annotations

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector
from parsimony.errors import EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient, map_http_error, map_timeout_error

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_CLOB_BASE = "https://clob.polymarket.com"

_MARKETS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="polymarket"),
        Column(name="question", role=ColumnRole.TITLE),
        Column(name="slug", role=ColumnRole.METADATA),
        Column(name="active", role=ColumnRole.METADATA),
    ]
)

_EVENTS_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="polymarket"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="slug", role=ColumnRole.METADATA),
    ]
)


def _gamma_http() -> HttpClient:
    return HttpClient(_GAMMA_BASE, timeout=15.0)


def _clob_http() -> HttpClient:
    return HttpClient(_CLOB_BASE, timeout=15.0)


async def _get_json(http: HttpClient, path: str, *, params: dict[str, object] | None = None) -> object:
    try:
        response = await http.request("GET", path, params=params)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="polymarket", op_name=path)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider="polymarket", op_name=path)
    return response.json()


@connector(output=_MARKETS_OUTPUT, tags=["polymarket", "tool"])
async def polymarket_markets(limit: int = 20, active: bool = True) -> pd.DataFrame:
    """List Polymarket markets from the Gamma API."""
    if limit < 1 or limit > 100:
        raise InvalidParameterError("polymarket", "limit must be between 1 and 100")
    data = await _get_json(_gamma_http(), "/markets", params={"limit": limit, "active": str(active).lower()})
    if not isinstance(data, list) or not data:
        raise EmptyDataError(provider="polymarket", message="No Polymarket markets returned")
    df = pd.DataFrame(data)
    keep = [c.name for c in _MARKETS_OUTPUT.columns if c.name in df.columns]
    return df[keep].head(limit)


@connector(output=_EVENTS_OUTPUT, tags=["polymarket", "tool"])
async def polymarket_events(limit: int = 20) -> pd.DataFrame:
    """List Polymarket events from the Gamma API."""
    if limit < 1 or limit > 100:
        raise InvalidParameterError("polymarket", "limit must be between 1 and 100")
    data = await _get_json(_gamma_http(), "/events", params={"limit": limit})
    if not isinstance(data, list) or not data:
        raise EmptyDataError(provider="polymarket", message="No Polymarket events returned")
    df = pd.DataFrame(data)
    keep = [c.name for c in _EVENTS_OUTPUT.columns if c.name in df.columns]
    return df[keep].head(limit)


@connector(tags=["polymarket", "tool"])
async def polymarket_market_prices(token_id: str) -> dict[str, object]:
    """Fetch the current CLOB price for a Polymarket outcome token."""
    token = token_id.strip()
    if not token:
        raise InvalidParameterError("polymarket", "token_id is required")
    data = await _get_json(_clob_http(), "/price", params={"token_id": token, "side": "buy"})
    if not isinstance(data, dict):
        raise EmptyDataError(provider="polymarket", message=f"No price returned for token_id={token!r}")
    return data


CONNECTORS = Connectors([polymarket_markets, polymarket_events, polymarket_market_prices])

__all__ = ["CONNECTORS"]
