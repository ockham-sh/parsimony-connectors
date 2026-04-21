"""FRED (Federal Reserve Economic Data) connector for parsimony.

Exports:

* :data:`CONNECTORS` — the :class:`parsimony.Connectors` collection exposed
  via the ``parsimony.providers`` entry point. Includes ``fred_search``
  (tool-tagged for MCP), ``fred_fetch``, ``enumerate_fred``,
  and ``enumerate_fred_release``.
* :data:`ENV_VARS` — maps the ``api_key`` dependency to ``FRED_API_KEY``.
* :data:`CATALOGS` — publish-target list consumed by
  :func:`parsimony.publish.publish`. One catalog named ``fred`` backed by
  :func:`enumerate_fred` (param-less; walks every FRED release).
* :func:`enumerate_fred` — param-less catalog enumerator.
* :func:`enumerate_fred_release` — per-release catalog enumerator (retains
  targeted indexing for users who want a single release).
"""

from __future__ import annotations

from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
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
    "CATALOGS",
    "CONNECTORS",
    "ENV_VARS",
    "PROVIDER_METADATA",
    "FredSearchParams",
    "FredFetchParams",
    "FredEnumerateParams",
    "FredEnumerateAllParams",
    "fred_search",
    "fred_fetch",
    "enumerate_fred",
    "enumerate_fred_release",
]

__version__ = "0.3.0"

ENV_VARS: dict[str, str] = {"api_key": "FRED_API_KEY"}

PROVIDER_METADATA: dict[str, Any] = {
    "homepage": "https://fred.stlouisfed.org",
    "pricing": "free",
    "rate_limits": "120 requests/min with a free API key",
}

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


class FredEnumerateParams(BaseModel):
    """Parameters for enumerating FRED series in a release (catalog indexing)."""

    release_id: int = Field(..., ge=1, description="FRED release ID")


class FredEnumerateAllParams(BaseModel):
    """No parameters — :func:`enumerate_fred` walks every release.

    Declared as an empty pydantic model rather than ``None`` so the kernel's
    ``Connector.param_type()()`` construction in
    :func:`parsimony.publish.publish` succeeds without args.
    """


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

FRED_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="units_short", role=ColumnRole.METADATA),
        Column(name="frequency_short", role=ColumnRole.METADATA),
        Column(name="seasonal_adjustment_short", role=ColumnRole.METADATA),
        Column(name="release_id", role=ColumnRole.METADATA),
    ]
)

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


@connector(tags=["macro", "tool"])
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


@connector(output=FETCH_OUTPUT, tags=["macro"])
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


# ---------------------------------------------------------------------------
# Catalog enumeration
# ---------------------------------------------------------------------------


async def _fetch_release_series_page(
    http: HttpClient,
    release_id: int,
    *,
    limit: int = 1000,
    offset: int = 0,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "release_id": release_id,
        "limit": limit,
        "offset": offset,
        "file_type": "json",
    }
    response = await http.request("GET", "/release/series", params=params)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="fred", op_name="release/series")
    return response.json().get("seriess") or []


async def _enumerate_release_series(
    http: HttpClient,
    release_id: int,
    *,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    all_series: list[dict[str, Any]] = []
    offset = 0
    while True:
        batch = await _fetch_release_series_page(http, release_id, limit=page_size, offset=offset)
        if not batch:
            break
        all_series.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_series


async def _list_releases(http: HttpClient, *, page_size: int = 1000) -> list[int]:
    """Enumerate every FRED release id.

    FRED currently hosts <400 releases so one page is typically sufficient,
    but we paginate defensively.
    """
    ids: list[int] = []
    offset = 0
    while True:
        response = await http.request(
            "GET",
            "/releases",
            params={"limit": page_size, "offset": offset, "file_type": "json"},
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="fred", op_name="releases")
        batch = response.json().get("releases") or []
        if not batch:
            break
        for row in batch:
            try:
                ids.append(int(row["id"]))
            except (KeyError, TypeError, ValueError):
                continue
        if len(batch) < page_size:
            break
        offset += page_size
    return ids


def _series_row(series: dict[str, Any], release_id: int) -> dict[str, Any] | None:
    series_id = str(series.get("id", "")).strip()
    if not series_id:
        return None
    title = str(series.get("title", "")).strip() or series_id
    return {
        "series_id": series_id,
        "title": title,
        "frequency_short": series.get("frequency_short"),
        "units_short": series.get("units_short"),
        "seasonal_adjustment_short": series.get("seasonal_adjustment_short"),
        "release_id": release_id,
    }


@enumerator(
    output=FRED_ENUMERATE_OUTPUT,
    tags=["fred"],
)
async def enumerate_fred_release(
    params: FredEnumerateParams,
    *,
    api_key: str,
) -> pd.DataFrame:
    """Enumerate FRED series for a single release (catalog indexing).

    Return one row per series in the release with id, title, and metadata columns.
    """
    http = _make_http(api_key)
    seriess = await _enumerate_release_series(http, params.release_id, page_size=1000)
    rows: list[dict[str, Any]] = []
    for item in seriess:
        row = _series_row(item, params.release_id)
        if row is not None:
            rows.append(row)
    return pd.DataFrame(rows)


@enumerator(
    output=FRED_ENUMERATE_OUTPUT,
    tags=["fred"],
)
async def enumerate_fred(
    params: FredEnumerateAllParams,
    *,
    api_key: str,
) -> pd.DataFrame:
    """Enumerate every FRED series across every release (catalog indexing).

    Walks ``/releases`` then ``/release/series`` for each release. Used by
    the ``parsimony publish --provider fred`` canonical catalog build —
    single namespace ``fred``, one row per series. Duplicate series (a
    series belonging to multiple releases) are kept on their first-seen
    release to preserve per-series ``release_id`` provenance.
    """
    http = _make_http(api_key)
    release_ids = await _list_releases(http)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for release_id in release_ids:
        seriess = await _enumerate_release_series(http, release_id, page_size=1000)
        for item in seriess:
            row = _series_row(item, release_id)
            if row is None:
                continue
            if row["series_id"] in seen:
                continue
            seen.add(row["series_id"])
            rows.append(row)
    return pd.DataFrame(rows)


CONNECTORS = Connectors([fred_search, fred_fetch, enumerate_fred, enumerate_fred_release])

#: Publish target: one canonical ``fred`` catalog covering every release.
CATALOGS: list[tuple[str, Any]] = [("fred", enumerate_fred)]
