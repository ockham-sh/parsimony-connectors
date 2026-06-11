"""BdE series fetch connector.

Banco de España BIEST is a **keyless** public JSON API — no api_key, no
``secrets=``/``bind()``/``load()``, no ``UnauthorizedError``. The
``listaSeries`` endpoint returns long-format JSON (``fechas``/``valores``
parallel arrays) which we reshape into one row per (series, observation).
"""

from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json, make_http_client

from parsimony_bde._http import BASE_URL
from parsimony_bde.outputs import BDE_FETCH_OUTPUT

_VALID_RANGES = frozenset({"30M", "60M", "MAX"})
_VALID_LANGS = frozenset({"en", "es"})


def _validate_time_range(time_range: str | None) -> str | None:
    """Normalise and validate the ``time_range`` argument.

    Accepts ``30M``/``60M``/``MAX`` (case-insensitive) or a 4-digit year.
    Returns the validated value, or ``None`` for the default full range.
    """
    if time_range is None:
        return None
    value = time_range.strip()
    if not value:
        return None
    if value.upper() in _VALID_RANGES:
        return value.upper()
    if value.isdigit():
        return value
    raise InvalidParameterError("bde", f"Invalid time_range '{time_range}'. Use 30M, 60M, MAX, or a year (e.g. 2024).")


def _parse_bde_response(json_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Parse BdE long-format JSON into a flat ``key,title,date,value`` frame."""
    all_rows: list[dict[str, Any]] = []

    for series in json_data:
        key = series.get("serie", "")
        title = series.get("descripcionCorta") or series.get("descripcion") or key
        dates = series.get("fechas", [])
        values = series.get("valores", [])

        if not dates or not values:
            continue

        for date_str, raw_value in zip(dates, values, strict=False):
            try:
                value = float(raw_value) if raw_value not in (None, "", "NaN") else None
            except (ValueError, TypeError):
                value = None

            date_val = date_str
            if isinstance(date_str, str) and "T" in date_str:
                with contextlib.suppress(ValueError):
                    date_val = datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%Y-%m-%d")

            all_rows.append({"key": key, "title": title, "date": date_val, "value": value})

    if not all_rows:
        return pd.DataFrame(columns=["key", "title", "date", "value"])
    return pd.DataFrame(all_rows)


@connector(output=BDE_FETCH_OUTPUT, tags=["macro", "es"])
def bde_fetch(
    key: Annotated[str, Namespace("bde")],
    time_range: str | None = None,
    lang: str = "en",
) -> pd.DataFrame:
    """Fetch Banco de España time series by series code(s).

    ``key`` is one or more comma-separated BdE series codes (e.g.
    ``D_1NBAF472`` or ``D_1NBAF472,DTCCBCEUSDEUR.B``); all codes are fetched in
    a single request. ``time_range`` accepts ``30M``/``60M``/``MAX`` or a year
    (e.g. ``2024``); ``None`` returns the full available range. ``lang`` selects
    the title/description language (``en`` or ``es``). Returns one row per
    observation with ``key``, ``title``, ``date``, ``value``.
    """
    keys = [k.strip() for k in key.split(",") if k.strip()]
    if not keys:
        raise InvalidParameterError("bde", "At least one series code required")
    if lang not in _VALID_LANGS:
        raise InvalidParameterError("bde", "lang must be 'en' or 'es'")
    resolved_range = _validate_time_range(time_range)

    req_params: dict[str, Any] = {
        "idioma": lang,
        "series": ",".join(keys),
        "rango": resolved_range,
    }
    body = fetch_json(
        make_http_client(BASE_URL, timeout=60.0),
        path="listaSeries",
        params=req_params,
        provider="bde",
        op_name="series",
    )

    if not isinstance(body, list):
        raise ParseError("bde", f"unexpected response shape for series: {','.join(keys)}")
    if not body:
        raise EmptyDataError(
            "bde",
            message=f"BdE returned no series for: {','.join(keys)}",
            query_params={"key": key, "time_range": time_range, "lang": lang},
        )

    df = _parse_bde_response(body)
    if df.empty:
        raise EmptyDataError(
            "bde",
            message=f"No observations parsed for: {','.join(keys)}",
            query_params={"key": key, "time_range": time_range, "lang": lang},
        )

    return df
