"""FRED (Federal Reserve Economic Data) connector for parsimony.

Exports:

* :data:`CONNECTORS` — the :class:`parsimony.Connectors` collection exposed
  via the ``parsimony.providers`` entry point. Includes ``fred_search``
  (tool-tagged for MCP) and ``fred_fetch``.
"""

from __future__ import annotations

from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport import HttpClient, map_http_error
from pydantic import BaseModel, Field, field_validator

__all__ = [
    "CONNECTORS",
    "FredSearchParams",
    "FredFetchParams",
    "fred_search",
    "fred_fetch",
]

# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class FredSearchParams(BaseModel):
    """Parameters for FRED keyword search."""

    search_text: str = Field(..., min_length=1, description="Search query")


class FredFetchParams(BaseModel):
    """Parameters for fetching FRED time series observations."""

    series_id: Annotated[str, "ns:fred"] = Field(..., description="FRED series identifier (e.g. GDPC1, UNRATE)")
    observation_start: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    observation_end: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        stripped = str(value).strip()
        if not stripped:
            raise ValueError("series_id must be non-empty")
        return stripped


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

_FRED_IDENTITY_METADATA: list[Column] = [
    Column(
        name="series_id",
        role=ColumnRole.KEY,
        param_key="series_id",
        namespace="fred",
    ),
    Column(name="title", role=ColumnRole.TITLE),
    Column(name="units_short", role=ColumnRole.METADATA),
    Column(name="frequency_short", role=ColumnRole.METADATA),
    Column(name="seasonal_adjustment_short", role=ColumnRole.METADATA),
]

FETCH_OUTPUT = OutputConfig(
    columns=[
        *_FRED_IDENTITY_METADATA,
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

SEARCH_COLUMNS = [
    "id",
    "title",
    "units",
    "frequency_short",
    "seasonal_adjustment_short",
    "observation_start",
    "observation_end",
    "last_updated",
]


def _make_http(api_key: str) -> HttpClient:
    return HttpClient(
        "https://api.stlouisfed.org/fred",
        query_params={"api_key": api_key, "file_type": "json"},
    )


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(env={"api_key": "FRED_API_KEY"}, tags=["macro", "tool"])
async def fred_search(params: FredSearchParams, *, api_key: str) -> Result:
    """Keyword search for FRED economic time series.

    Returns series metadata (id, title, units, frequency).
    Use short, specific queries like 'US unemployment rate' or 'GDPC1'.
    """
    http = _make_http(api_key)
    response = await http.request(
        "GET",
        "/series/search",
        params={"search_text": params.search_text},
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="fred", op_name="series/search")
    seriess = response.json().get("seriess", [])
    if not seriess:
        raise EmptyDataError(provider="fred", message=f"No series found for: {params.search_text}")
    df = pd.DataFrame(seriess)
    cols = [c for c in SEARCH_COLUMNS if c in df.columns]
    df = df[cols]
    return Result.from_dataframe(
        df,
        Provenance(source="fred", params={"search_text": params.search_text}),
    )


@connector(env={"api_key": "FRED_API_KEY"}, output=FETCH_OUTPUT, tags=["macro"])
async def fred_fetch(params: FredFetchParams, *, api_key: str) -> Result:
    """Fetch FRED time series observations by series_id.

    Returns date + value columns with rich metadata (title, units, frequency, seasonal adjustment).
    """
    http = _make_http(api_key)
    series_id = params.series_id

    req_params: dict[str, Any] = {"series_id": series_id}
    if params.observation_start is not None:
        req_params["observation_start"] = params.observation_start
    if params.observation_end is not None:
        req_params["observation_end"] = params.observation_end

    obs_response = await http.request("GET", "/series/observations", params=req_params)
    try:
        obs_response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="fred", op_name="series/observations")
    obs_data = obs_response.json()["observations"]

    series_response = await http.request("GET", "/series", params={"series_id": series_id})
    try:
        series_response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="fred", op_name="series")
    series_data = series_response.json()["seriess"][0]

    df = pd.DataFrame(obs_data)
    df["series_id"] = series_id
    df["title"] = str(series_data.get("title", ""))
    df["units_short"] = series_data.get("units_short")
    df["frequency_short"] = series_data.get("frequency_short")
    df["seasonal_adjustment_short"] = series_data.get("seasonal_adjustment_short")

    meta_keys = [
        ("id", False),
        ("title", False),
        ("units", False),
        ("units_short", True),
        ("frequency", False),
        ("frequency_short", False),
        ("seasonal_adjustment", False),
        ("seasonal_adjustment_short", True),
        ("last_updated", False),
        ("notes", True),
    ]
    metadata_list = [
        {"name": k, "value": str(series_data[k]), "exclude_from_llm_view": excl}
        for k, excl in meta_keys
        if k in series_data
    ]
    metadata_list.append(
        {
            "name": "series_url",
            "value": f"https://fred.stlouisfed.org/series/{series_id}",
        }
    )

    prov_params: dict[str, Any] = {"series_id": series_id}
    if params.observation_start is not None:
        prov_params["observation_start"] = params.observation_start
    if params.observation_end is not None:
        prov_params["observation_end"] = params.observation_end

    prov = Provenance(
        source="fred",
        params=prov_params,
        properties={"metadata": metadata_list},
    )
    return FETCH_OUTPUT.build_table_result(
        df,
        provenance=prov,
        params=prov_params,
    )


CONNECTORS = Connectors([fred_search, fred_fetch])
