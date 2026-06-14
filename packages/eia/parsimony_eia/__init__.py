"""US Energy Information Administration (EIA): fetch + catalog enumeration.

API docs: https://www.eia.gov/opendata/documentation.php

The API key is declared as a secret and supplied by binding
(``load(api_key=...)`` / ``Connector.bind``) or, as a dev fallback, from the
``EIA_API_KEY`` environment variable. A missing key fails fast with
:class:`UnauthorizedError`.
"""

from __future__ import annotations

from typing import Annotated

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient
from parsimony.transport.helpers import fetch_json, make_api_key_client, require_key

__all__ = ["CONNECTORS", "load"]

_BASE_URL = "https://api.eia.gov/v2"
_ENV_VAR = "EIA_API_KEY"

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

EIA_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="route", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
    ]
)

EIA_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="route", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="period", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _client(api_key: str) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the EIA client."""
    key = require_key(api_key, env_var=_ENV_VAR, provider="eia")
    return make_api_key_client(_BASE_URL, api_key=key, api_key_param="api_key", timeout=30.0)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=EIA_FETCH_OUTPUT, tags=["macro", "energy", "us"], secrets=("api_key",))
def eia_fetch(
    route: Annotated[str, Namespace("eia")],
    measure: str = "value",
    frequency: str | None = None,
    start: str | None = None,
    end: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch EIA energy data by API route (e.g. petroleum/pri/spt).

    `measure` selects EIA v2's required data facet — it is route-dependent
    (e.g. "value" for petroleum prices; "sales"/"revenue"/"price"/"customers"
    for electricity/retail-sales). The selected measure is normalized to a
    `value` column. `period` is parsed to datetime; other columns retain their
    original EIA names (folded in as data).
    """
    r = route.strip()
    if not r:
        raise InvalidParameterError("eia", "route must be non-empty")
    m = measure.strip()
    if not m:
        raise InvalidParameterError("eia", "measure must be non-empty")

    http = _client(api_key)
    body = fetch_json(
        http,
        path=f"{r}/data",
        # data[0]=<measure> is REQUIRED for EIA v2 to return the measure column;
        # without it the API returns only dimension columns. The measure id is
        # route-specific, hence the `measure` parameter (default "value").
        params={"data[0]": m, "frequency": frequency, "start": start, "end": end},
        provider="eia",
        op_name="eia_fetch",
    )

    resp = body.get("response", {})
    data = resp.get("data", [])
    if not data:
        raise EmptyDataError("eia", query_params={"route": r, "measure": m})

    df = pd.DataFrame(data)
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], errors="coerce", format="mixed")
    # Normalize the selected measure to a stable `value` column. Coerce only it —
    # the previous version coerced *every* column, silently NaN-ing string
    # metadata like duoarea/product.
    if m != "value" and m in df.columns:
        df = df.rename(columns={m: "value"})
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df["route"] = r
    df["title"] = resp.get("description", r)
    return df


@enumerator(output=EIA_ENUMERATE_OUTPUT, tags=["macro", "energy", "us"], secrets=("api_key",))
def enumerate_eia(api_key: str = "") -> pd.DataFrame:
    """Enumerate top-level EIA API routes for catalog indexing."""
    http = _client(api_key)
    body = fetch_json(http, path="", provider="eia", op_name="enumerate_eia")

    routes = body.get("response", {}).get("routes", [])
    rows = [
        {
            "route": route.get("id", ""),
            "title": route.get("name", route.get("id", "")),
            "description": route.get("description", ""),
        }
        for route in routes
    ]
    if not rows:
        raise EmptyDataError("eia", query_params={})

    return pd.DataFrame(rows)


CONNECTORS = Connectors([eia_fetch, enumerate_eia])


def load(*, api_key: str) -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
