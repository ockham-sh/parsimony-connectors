"""FRED (Federal Reserve Economic Data) connector for parsimony.

Exports:

* :data:`CONNECTORS` — the :class:`parsimony.Connectors` collection exposed
  via the ``parsimony.providers`` entry point. Includes ``fred_search``
  (tool-tagged for agent use) and ``fred_fetch``.
* :func:`load` — convenience that binds an ``api_key`` across the collection.

The API key is declared as a secret (stripped from provenance) and supplied
either by binding (``load(api_key=...)`` / ``Connector.bind``) or, as a dev
fallback, from the ``FRED_API_KEY`` environment variable. A missing key
fails fast with :class:`UnauthorizedError` naming the env var.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
)
from parsimony.result import Column, ColumnRole, OutputSpec
from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import make_http_client, require_key

__all__ = ["CONNECTORS", "load"]

_BASE_URL = "https://api.stlouisfed.org/fred"
_ENV_VAR = "FRED_API_KEY"

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units_short", role=ColumnRole.METADATA),
        Column(name="frequency_short", role=ColumnRole.METADATA),
        Column(name="seasonal_adjustment_short", role=ColumnRole.METADATA),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units", role=ColumnRole.METADATA),
        Column(name="frequency_short", role=ColumnRole.METADATA),
        Column(name="seasonal_adjustment_short", role=ColumnRole.METADATA),
        Column(name="observation_start", role=ColumnRole.METADATA),
        Column(name="observation_end", role=ColumnRole.METADATA),
        Column(name="last_updated", role=ColumnRole.METADATA),
    ]
)

_SEARCH_COLUMNS = [c.name for c in SEARCH_OUTPUT.columns]


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _client(api_key: str) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build a FRED HTTP client.

    The key and ``file_type=json`` are set as default query params on every
    request; the transport layer redacts the key from logs. A missing key
    raises :class:`UnauthorizedError` before any network call.
    """
    key = require_key(api_key, env_var=_ENV_VAR, provider="fred")
    return make_http_client(_BASE_URL, provider="fred", query_params={"api_key": key, "file_type": "json"})


def _get_json(http: HttpClient, *, path: str, params: dict[str, Any], op_name: str) -> Any:
    """GET *path* and return parsed JSON, surfacing FRED's actionable 400 body.

    FRED reports a bad ``series_id`` or parameter as a clean HTTP 400 whose
    ``error_message`` names the problem ("Invalid value for variable series_id",
    "Invalid value for parameter frequency"). The shared ``check_status`` maps a
    400 to an opaque ``ProviderError`` from the status alone, dropping that text,
    so it is read here and raised as a message-preserving ``InvalidParameterError``;
    every other status defers to the canonical mapping. Kept local (mirroring the
    EIA chokepoint) so the framework's shared ``fetch_json`` stays provider-agnostic.
    """
    filtered = {k: v for k, v in params.items() if v is not None}
    response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    if response.status_code == 400:
        try:
            message = response.json().get("error_message")
        except ValueError:
            message = None
        if isinstance(message, str) and message.strip():
            raise InvalidParameterError("fred", message.strip()[:300])
    check_status(response, provider="fred", op_name=op_name)
    try:
        return response.json()
    except ValueError as exc:
        raise ParseError("fred", f"fred: '{op_name}' returned a non-JSON response body") from exc


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=SEARCH_OUTPUT, tags=["macro", "tool"], secrets=("api_key",))
def fred_search(query: str, api_key: str = "") -> pd.DataFrame:
    """Keyword search for FRED economic time series.

    Returns series metadata (id, title, units, frequency). Use short,
    specific queries like 'US unemployment rate' or 'GDPC1'.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError("fred", "query must be non-empty")

    http = _client(api_key)
    body = _get_json(
        http,
        path="series/search",
        # FRED's own wire parameter is `search_text`; the connector exposes it as
        # `query`, the name every other *_search connector uses.
        params={"search_text": q},
        op_name="series/search",
    )

    seriess = body.get("seriess") or []
    if not seriess:
        raise EmptyDataError("fred", query_params={"query": q})

    df = pd.DataFrame(seriess)
    cols = [c for c in _SEARCH_COLUMNS if c in df.columns]
    return df[cols]


@connector(output=FETCH_OUTPUT, tags=["macro"], secrets=("api_key",))
def fred_fetch(
    series_id: Annotated[str, Namespace("fred")],
    observation_start: str | None = None,
    observation_end: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch FRED time series observations by series_id.

    Returns date + value rows enriched with series metadata (title, units,
    frequency, seasonal adjustment). Optional observation_start/observation_end
    bound the window (YYYY-MM-DD).
    """
    sid = series_id.strip()
    if not sid:
        raise InvalidParameterError("fred", "series_id must be non-empty")

    http = _client(api_key)
    obs_body = _get_json(
        http,
        path="series/observations",
        params={
            "series_id": sid,
            "observation_start": observation_start,
            "observation_end": observation_end,
        },
        op_name="series/observations",
    )

    observations = obs_body.get("observations")
    if observations is None:
        raise ParseError("fred", "FRED response missing 'observations'")
    if not observations:
        raise EmptyDataError("fred", query_params={"series_id": sid})

    meta_body = _get_json(
        http,
        path="series",
        params={"series_id": sid},
        op_name="series",
    )
    seriess = meta_body.get("seriess") or []
    meta = seriess[0] if seriess else {}

    df = pd.DataFrame(observations)
    df["series_id"] = sid
    df["title"] = str(meta.get("title", ""))
    df["units_short"] = meta.get("units_short")
    df["frequency_short"] = meta.get("frequency_short")
    df["seasonal_adjustment_short"] = meta.get("seasonal_adjustment_short")
    # Return only the declared schema columns — FRED observation rows also carry
    # realtime_start/realtime_end, which would otherwise be folded in as stray DATA.
    return df[[c.name for c in FETCH_OUTPUT.columns]]


CONNECTORS = Connectors([fred_search, fred_fetch])


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
