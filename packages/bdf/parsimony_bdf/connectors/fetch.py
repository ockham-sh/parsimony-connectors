"""BdF series fetch connector (Webstat ``observations`` export).

Pulls observation rows for a single SDMX ``series_key`` from the Opendatasoft
``observations`` dataset, filtered server-side with an ODSQL ``where`` clause.
``obs_value`` is null on missing-status rows (BdF marks gaps with ``OBS_STATUS=M``),
so values may legitimately be ``None`` between real observations.
"""

from __future__ import annotations

from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_bdf._http import OBSERVATIONS_PATH, OBSERVATIONS_SELECT, make_fetch_client
from parsimony_bdf.outputs import BDF_FETCH_OUTPUT, FETCH_COLUMNS


def _validate_period(value: str | None, name: str) -> str | None:
    """Screen a ``YYYY-MM-DD`` period bound before it enters the ODSQL clause.

    Returns the validated string, or ``None`` when unset. A malformed value is
    rejected pre-network as :class:`InvalidParameterError` rather than being
    embedded in ``date'…'`` for the server to reject with a generic 400.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    parts = text.split("-")
    if len(parts) == 3 and parts[0].isdigit() and len(parts[0]) == 4 and all(p.isdigit() for p in parts[1:]):
        return text
    raise InvalidParameterError("bdf", f"{name} must be an ISO date 'YYYY-MM-DD' (got {value!r})")


def _build_where(series_key: str, start: str | None, end: str | None) -> str:
    where = f'series_key="{series_key}"'
    if start:
        where += f" and time_period_start>=date'{start}'"
    if end:
        where += f" and time_period_start<=date'{end}'"
    return where


def _parse_observations(payload: Any, series_key: str) -> pd.DataFrame:
    """Reshape the flat observations array into ``key,title,date,value`` rows."""
    if not isinstance(payload, list):
        raise ParseError("bdf", f"unexpected response shape for key: {series_key}")
    if not payload:
        raise EmptyDataError("bdf", query_params={"key": series_key})

    rows: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        date_str = str(row.get("time_period_start") or "").strip()
        if not date_str:
            continue
        raw_value = row.get("obs_value")
        try:
            value = float(raw_value) if raw_value is not None else None
        except (ValueError, TypeError):
            value = None
        title = str(row.get("title_en") or row.get("title_fr") or row.get("series_key") or series_key)
        rows.append(
            {
                "key": str(row.get("series_key") or series_key),
                "title": title,
                "date": date_str,
                "value": value,
            }
        )

    if not rows:
        raise EmptyDataError("bdf", query_params={"key": series_key})
    df = pd.DataFrame(rows, columns=list(FETCH_COLUMNS))
    df["date"] = pd.to_datetime(df["date"])
    return df


@connector(output=BDF_FETCH_OUTPUT, tags=["macro", "fr"], secrets=("api_key",), requires=("BDF_API_KEY",))
def bdf_fetch(
    key: Annotated[str, "ns:bdf"],
    start_period: str | None = None,
    end_period: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch Banque de France time series via the Webstat Opendatasoft API.

    Pulls observation rows for a single dot-separated SDMX series ``key`` (e.g.
    ``EXR.M.USD.EUR.SP00.E``) and returns ``(key, title, date, value)`` rows
    ordered by date. Optional ``start_period`` / ``end_period`` (``YYYY-MM-DD``)
    bound the result on ``time_period_start``. Discover keys with ``bdf_search``
    or ``enumerate_bdf``. Missing-status gaps come back with a null ``value``.
    """
    series_key = key.strip()
    if not series_key:
        raise InvalidParameterError("bdf", "key must be non-empty")
    start = _validate_period(start_period, "start_period")
    end = _validate_period(end_period, "end_period")

    http = make_fetch_client(api_key)
    payload = fetch_json(
        http,
        path=OBSERVATIONS_PATH,
        params={
            "select": OBSERVATIONS_SELECT,
            "where": _build_where(series_key, start, end),
            "order_by": "time_period_start",
        },
        op_name="observations",
    )
    return _parse_observations(payload, series_key)


__all__ = ["bdf_fetch"]
