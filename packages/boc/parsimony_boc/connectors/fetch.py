"""BoC observations fetch connector (Valet ``/observations`` endpoint).

Fetches one or more series by name, or a whole named panel via ``group:NAME``.
The observations payload is a wide, date-keyed array — ``{"d": ..., "FXUSDCAD":
{"v": "1.38"}, ...}`` — which we melt into long ``(series_name, title, date,
value)`` rows. Suppressed/missing observations (``""`` / ``"NaN"`` / absent) come
back as a null ``value``.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_boc._http import PROVIDER, guard_observations_path, make_valet_client
from parsimony_boc.outputs import BOC_FETCH_OUTPUT

_FETCH_COLUMNS = ["series_name", "title", "date", "value"]


def _parse_observations(
    json_data: dict[str, Any],
    series_details: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Melt the Valet observations array into long-format rows.

    Each observation entry looks like
    ``{"d": "2024-01-15", "FXUSDCAD": {"v": "1.3456"}, ...}`` — one date key
    plus one nested ``{"v": ...}`` per series. Titles are resolved from the
    payload's ``seriesDetail`` block when present, else fall back to the name.
    """
    observations = json_data.get("observations", [])
    if not observations:
        return pd.DataFrame(columns=_FETCH_COLUMNS)

    sample = observations[0]
    series_cols = [k for k in sample if k != "d"]

    rows: list[dict[str, Any]] = []
    for obs in observations:
        date = obs.get("d", "")
        for col in series_cols:
            raw = obs.get(col)
            if raw is None:
                continue
            raw_value = raw.get("v") if isinstance(raw, dict) else raw
            try:
                value = float(raw_value) if raw_value is not None and raw_value not in ("", "NaN") else None
            except (ValueError, TypeError):
                value = None

            title = col
            if series_details and col in series_details:
                detail = series_details[col]
                title = detail.get("label", detail.get("description", col))

            rows.append({"series_name": col, "title": title, "date": date, "value": value})

    return pd.DataFrame(rows, columns=_FETCH_COLUMNS) if rows else pd.DataFrame(columns=_FETCH_COLUMNS)


@connector(output=BOC_FETCH_OUTPUT, tags=["macro", "ca"])
def boc_fetch(
    series_name: Annotated[str, Namespace("boc")],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch Bank of Canada time series by series name(s) or group name.

    Use 'group:GROUP_NAME' syntax for a whole panel (e.g. group:FX_RATES_DAILY).
    Otherwise pass one or more comma-separated series names (e.g.
    FXUSDCAD,FXEURCAD). Discover names with boc_search or enumerate_boc.
    Observations come back as long-format (series_name, title, date, value)
    rows; optional start_date/end_date (YYYY-MM-DD) bound the window.
    """
    series_name = series_name.strip()
    if not series_name:
        raise InvalidParameterError(PROVIDER, "series_name must be non-empty")

    if series_name.startswith("group:"):
        group_name = series_name[6:].strip()
        if not group_name:
            raise InvalidParameterError(PROVIDER, "group name must be non-empty after 'group:'")
        path = f"observations/group/{group_name}/json"
    else:
        path = f"observations/{series_name}/json"

    # Valet 302-redirects observations requests whose URL exceeds ~4 KB; reject
    # those pre-network with actionable guidance rather than a downstream parse
    # failure. Only the multi-series path realistically trips this.
    guard_observations_path(path, series_name=series_name)

    json_data = fetch_json(
        make_valet_client(),
        path=path,
        params={"start_date": start_date, "end_date": end_date},
        op_name="observations",
    )
    if not isinstance(json_data, dict):
        raise ParseError(PROVIDER, f"unexpected observations response shape for: {series_name}")

    df = _parse_observations(json_data, json_data.get("seriesDetail"))
    if df.empty:
        raise EmptyDataError(
            PROVIDER,
            message=f"No observations returned for: {series_name}",
            query_params={"series_name": series_name, "start_date": start_date, "end_date": end_date},
        )

    df["date"] = pd.to_datetime(df["date"])
    return df


__all__ = ["boc_fetch"]
