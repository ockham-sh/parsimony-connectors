"""US Bureau of Labor Statistics: fetch + catalog enumeration.

API docs: https://www.bls.gov/developers/

BLS is a POST-body JSON API that signals failure in the response body
(HTTP 200 + a ``status`` field), not via HTTP status codes. The API key
(``registrationkey``) is **optional** — it only raises rate limits — so this
connector does not fast-fail on a missing key. The key is still declared as a
secret and stripped from provenance.
"""

from __future__ import annotations

import os
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import (
    ConnectorError,
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient, map_http_error, map_timeout_error, pooled_client
from parsimony.transport.helpers import fetch_json, make_http_client

__all__ = ["CONNECTORS", "load"]

_BASE_URL = "https://api.bls.gov/publicAPI/v2"
_ENV_VAR = "BLS_API_KEY"


# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

BLS_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="bls"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="survey", role=ColumnRole.METADATA),
    ]
)

BLS_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="bls"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _post_json(http: HttpClient, path: str, payload: dict[str, Any], *, provider: str, op_name: str) -> Any:
    """POST a JSON body and return parsed JSON, mapping transport failures to typed errors.

    The canonical POST helper: `fetch_json` is GET-only, so POST connectors use
    this — `raise_for_status()` + `map_http_error` / `map_timeout_error`. POST is
    not retried by the transport retry policy (non-idempotent), by design.
    """
    try:
        response = http.request("POST", path, json=payload)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=provider, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=provider, op_name=op_name)
    return response.json()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _period_to_date(year: str, period: str) -> str:
    """Convert a BLS year + period code to an ISO date string.

    BLS period codes: M01-M12 monthly; M13 = annual average; Q01-Q04 quarterly
    (Q05 = annual avg); S01/S02/S03 semiannual; A01 annual.
    """
    if period == "M13":  # annual average — represent at year end
        return f"{year}-12-31"
    if period.startswith("M") and len(period) == 3 and period[1:].isdigit():
        month = min(max(int(period[1:]), 1), 12)
        return f"{year}-{month:02d}-01"
    if period.startswith("Q") and len(period) == 3 and period[1:].isdigit():
        quarter = int(period[1:])
        if quarter >= 5:  # Q05 = annual
            return f"{year}-12-31"
        month = (quarter - 1) * 3 + 1
        return f"{year}-{month:02d}-01"
    if period.startswith("S") and len(period) == 3 and period[1:].isdigit():
        half = int(period[1:])
        return f"{year}-07-01" if half == 2 else f"{year}-01-01"
    return f"{year}-01-01"


def _infer_frequency(period: str) -> str:
    if period == "M13":
        return "Annual"
    if period.startswith("M"):
        return "Monthly"
    if period.startswith("Q"):
        return "Annual" if period == "Q05" else "Quarterly"
    if period.startswith("S"):
        return "Semiannual"
    if period.startswith("A"):
        return "Annual"
    return "Monthly"


def _validate_year(value: str, label: str) -> str:
    v = value.strip()
    if not v.isdigit() or len(v) != 4:
        raise InvalidParameterError("bls", f"{label} must be a 4-digit year (YYYY)")
    return v


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BLS_FETCH_OUTPUT, tags=["macro", "us"], secrets=("api_key",))
def bls_fetch(
    series_id: Annotated[str, Namespace("bls")],
    start_year: str,
    end_year: str,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a single BLS time series by series_id.

    Returns date + value rows with series metadata (title, frequency). The
    API key is optional but recommended for higher rate limits.
    """
    sid = series_id.strip()
    if not sid:
        raise InvalidParameterError("bls", "series_id must be non-empty")
    start = _validate_year(start_year, "start_year")
    end = _validate_year(end_year, "end_year")

    payload: dict[str, Any] = {
        "seriesid": [sid],
        "startyear": start,
        "endyear": end,
        "catalog": True,
    }
    resolved_key = api_key or os.environ.get(_ENV_VAR, "")
    if resolved_key:
        payload["registrationkey"] = resolved_key

    http = make_http_client(_BASE_URL, timeout=60.0)
    body = _post_json(http, "/timeseries/data/", payload, provider="bls", op_name="timeseries/data")

    # BLS reports logical failure in the body (HTTP 200 + a non-success status).
    status = body.get("status", "")
    if status != "REQUEST_SUCCEEDED":
        messages = [str(m) for m in (body.get("message") or [])]
        text = "; ".join(messages) or status
        if any(("threshold" in m.lower() or "daily" in m.lower()) for m in messages):
            raise RateLimitError(
                "bls",
                retry_after=3600.0,
                quota_exhausted=True,
                message=f"BLS query threshold reached: {text}",
            )
        raise ParseError("bls", f"BLS request not processed ({status}): {text}")

    series_list = body.get("Results", {}).get("series", [])
    if not series_list:
        raise EmptyDataError("bls", query_params={"series_id": sid})

    series_block = series_list[0]
    title = series_block.get("catalog", {}).get("series_title", sid)

    rows: list[dict[str, Any]] = []
    for obs in series_block.get("data", []):
        val_str = obs.get("value", "")
        try:
            value: float | None = float(val_str) if val_str not in ("-", "") else None
        except (ValueError, TypeError):
            value = None
        period = obs["period"]
        rows.append(
            {
                "series_id": sid,
                "title": title,
                "frequency": _infer_frequency(period),
                "date": _period_to_date(obs["year"], period),
                "value": value,
            }
        )

    if not rows:
        raise EmptyDataError("bls", query_params={"series_id": sid})

    return pd.DataFrame(rows)


@enumerator(output=BLS_ENUMERATE_OUTPUT, tags=["macro", "us"], secrets=("api_key",))
def enumerate_bls(survey: str = "", api_key: str = "") -> pd.DataFrame:
    """Enumerate popular BLS series, optionally limited to one survey code (e.g. 'CE').

    With no `survey`, crawls every survey's popular-series list (a large fan-out
    for catalog building). With a `survey` code, fetches just that survey — cheap.
    """
    resolved_key = api_key or os.environ.get(_ENV_VAR, "")
    query = {"registrationkey": resolved_key} if resolved_key else None
    http = make_http_client(_BASE_URL, query_params=query, timeout=60.0)

    if survey.strip():
        surveys: list[tuple[str, str]] = [(survey.strip(), survey.strip())]
    else:
        surveys_body = fetch_json(http, path="surveys", provider="bls", op_name="surveys")
        surveys = [
            (s.get("survey_abbreviation", ""), s.get("survey_name", ""))
            for s in surveys_body.get("Results", {}).get("survey", [])
        ]

    rows: list[dict[str, str]] = []
    with pooled_client(http) as shared:
        for code, name in surveys:
            if not code:
                continue
            try:
                popular = fetch_json(
                    shared,
                    path="timeseries/popular",
                    params={"survey": code},
                    provider="bls",
                    op_name="timeseries/popular",
                )
            except (RateLimitError, UnauthorizedError):
                # A quota wall / auth failure must abort loudly — returning a
                # half-built catalog that looks complete would be a surprise.
                raise
            except ConnectorError:
                # A single survey's transient/empty failure must not abort the
                # whole catalog crawl.
                continue
            for s in (popular.get("Results") or {}).get("series") or []:
                if not s:
                    continue
                sid = s.get("seriesID", "")
                if not sid:
                    continue
                rows.append(
                    {
                        "series_id": sid,
                        "title": s.get("seriesTitle") or s.get("title") or sid,
                        "survey": name,
                    }
                )

    if not rows:
        raise EmptyDataError("bls", query_params={"survey": survey})

    return pd.DataFrame(rows)


CONNECTORS = Connectors([bls_fetch, enumerate_bls])


def load(*, api_key: str = "") -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on every connector that accepts it."""
    return CONNECTORS.bind(api_key=api_key)
