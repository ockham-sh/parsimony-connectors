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

import httpx
import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport import check_status
from parsimony.transport.helpers import make_http_client

from parsimony_bde._http import BASE_URL
from parsimony_bde.outputs import BDE_FETCH_OUTPUT

# ``rango`` is frequency-dependent and BdE validates it server-side:
#   monthly / quarterly / semestral : 30M, 60M, MAX, or a 4-digit year
#   daily / business-daily          : 3M, 12M, 36M, or a 4-digit year (NOT MAX)
#   annual                          : 60M, MAX, or a 4-digit year
# A single request may mix frequencies, so the connector can't know the right
# vocabulary up front. We accept the union of literal codes here and let BdE
# reject a code that doesn't apply to a series' frequency — its HTTP 412 is
# surfaced as InvalidParameterError (see ``_check_bde_response``).
_VALID_RANGES = frozenset({"3M", "12M", "36M", "30M", "60M", "MAX"})
_VALID_LANGS = frozenset({"en", "es"})


def _validate_time_range(time_range: str | None) -> str | None:
    """Normalise and validate the ``time_range`` argument.

    Accepts ``3M``/``12M``/``36M``/``30M``/``60M``/``MAX`` (case-insensitive) or
    a 4-digit year. Which codes are valid depends on the series frequency (BdE
    enforces that), so this only screens out plainly malformed values. Returns
    the validated value, or ``None`` for the default range.
    """
    if time_range is None:
        return None
    value = time_range.strip()
    if not value:
        return None
    if value.upper() in _VALID_RANGES:
        return value.upper()
    if value.isdigit() and len(value) == 4:  # a 4-digit calendar year
        return value
    raise InvalidParameterError(
        "bde",
        f"Invalid time_range '{time_range}'. Use 3M/12M/36M/30M/60M/MAX "
        "(frequency-dependent) or a 4-digit year (e.g. 2024).",
    )


def _check_bde_response(response: httpx.Response, keys: list[str], *, op_name: str) -> None:
    """Map a BdE ``listaSeries`` response to a typed connector error, if any.

    BdE answers an invalid series code or a frequency-incompatible ``rango`` with
    HTTP 412 and a ``{"errNum", "errMsgUsr", "errMsgDebug"}`` body — a caller
    input problem, so it surfaces as :class:`InvalidParameterError` carrying
    BdE's own message. Everything else defers to ``check_status``.
    """
    if response.status_code == 412:
        detail = ""
        with contextlib.suppress(Exception):
            body = response.json()
            if isinstance(body, dict):
                detail = (body.get("errMsgDebug") or body.get("errMsgUsr") or "").strip()
        raise InvalidParameterError(
            "bde",
            detail or f"BdE rejected the request for series: {','.join(keys)}",
        )
    check_status(response, provider="bde", op_name=op_name)


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
    ``D_1NBAF472,DTCCBCEUSDEUR.B``); all are fetched in a single request.
    ``time_range`` is frequency-dependent: monthly/quarterly series take
    ``30M``/``60M``/``MAX``, daily series take ``3M``/``12M``/``36M`` (not
    ``MAX``), and any frequency accepts a 4-digit year (e.g. ``2024``); ``None``
    returns BdE's default range. A range that doesn't fit a series' frequency is
    rejected by BdE as an ``InvalidParameterError``. ``lang`` selects the title
    language (``en`` or ``es``). Returns one row per observation with ``key``,
    ``title``, ``date``, ``value``.
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
    http = make_http_client(BASE_URL, provider="bde", timeout=60.0)
    filtered = {k: v for k, v in req_params.items() if v is not None}
    response = http.request("GET", "/listaSeries", params=filtered or None, op_name="series")
    _check_bde_response(response, keys, op_name="series")
    try:
        body = response.json()
    except ValueError as exc:
        raise ParseError(provider="bde") from exc

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
    df["date"] = pd.to_datetime(df["date"])

    # BdE returns observations newest-first; sort ascending so downstream joins
    # don't need to re-sort. Sort by (key, date) so a multi-series request keeps
    # each series contiguous instead of interleaving them by date.
    return df.sort_values(["key", "date"], ignore_index=True)
