"""``bls_fetch`` — live observation retrieval from the BLS Public Data API.

The API is POST-with-JSON-body and signals logical failure **in the body**
(HTTP 200 + a ``status`` field), not via HTTP status codes. The
``registrationkey`` is optional — it only raises rate limits — so this connector
never fast-fails on a missing key. The key is still declared as a secret and
stripped from provenance.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, RateLimitError
from parsimony.transport.helpers import make_http_client

from parsimony_bls._http import API_BASE, API_TIMEOUT, post_api_json
from parsimony_bls.outputs import BLS_FETCH_OUTPUT


def _period_to_date(year: str, period: str) -> str:
    """Convert a BLS year + period code to an ISO date string.

    Period codes: ``M01``–``M12`` monthly (``M13`` = annual average); ``Q01``–``Q04``
    quarterly (``Q05`` = annual); ``S01``/``S02`` semiannual (``S03`` = annual);
    ``A01`` annual.
    """
    if period == "M13":
        return f"{year}-12-31"
    if period.startswith("M") and len(period) == 3 and period[1:].isdigit():
        month = min(max(int(period[1:]), 1), 12)
        return f"{year}-{month:02d}-01"
    if period.startswith("Q") and len(period) == 3 and period[1:].isdigit():
        quarter = int(period[1:])
        if quarter >= 5:
            return f"{year}-12-31"
        return f"{year}-{(quarter - 1) * 3 + 1:02d}-01"
    if period.startswith("S") and len(period) == 3 and period[1:].isdigit():
        half = int(period[1:])
        if half >= 3:
            return f"{year}-12-31"
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
        return "Annual" if period == "S03" else "Semiannual"
    if period.startswith("A"):
        return "Annual"
    return "Monthly"


def _validate_year(value: str, label: str) -> str:
    v = (value or "").strip()
    if not v.isdigit() or len(v) != 4:
        raise InvalidParameterError("bls", f"{label} must be a 4-digit year (YYYY)")
    return v


def _coerce_value(raw: Any) -> float | None:
    if raw in ("-", "", None):
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


@connector(output=BLS_FETCH_OUTPUT, tags=["macro", "us"], secrets=("api_key",))
def bls_fetch(
    series_id: Annotated[str, "ns:bls"],
    start_year: str,
    end_year: str,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a single BLS time series by ``series_id``.

    Returns date + value rows with series metadata (title, frequency). The api_key
    is optional but raises the daily quota and (when present) enriches the title
    from the API catalog. Reaches **any** series in the BLS universe by id.
    """
    sid = (series_id or "").strip()
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
    if api_key:
        payload["registrationkey"] = api_key

    http = make_http_client(API_BASE, timeout=API_TIMEOUT)
    body = post_api_json(http, "/timeseries/data/", payload, op_name="timeseries/data")

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
        period = obs.get("period", "")
        rows.append(
            {
                "series_id": sid,
                "title": title,
                "frequency": _infer_frequency(period),
                "date": _period_to_date(obs.get("year", ""), period),
                "value": _coerce_value(obs.get("value")),
            }
        )

    if not rows:
        raise EmptyDataError("bls", query_params={"series_id": sid})

    return pd.DataFrame(rows)


__all__ = ["bls_fetch"]
