"""``boj_fetch`` — fetch Bank of Japan time series by database + series code(s).

Wraps ``/getDataCode``. The one non-obvious behaviour is the **60,000-point
limit with ``NEXTPOSITION`` pagination**: a request whose ``(series × periods)``
exceeds 60,000 returns HTTP 200 ``"Successfully completed"`` carrying only the
first *K* series and a ``NEXTPOSITION`` cursor. We resume from that cursor
(``startPosition=NEXTPOSITION``) and accumulate series across pages, so a
multi-series request never silently drops its tail. Truncation is at a series
boundary (position-based), so the resume is lossless.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_boj._http import (
    MAX_CODES_PER_REQUEST,
    MAX_FETCH_PAGES,
    NEXTPOSITION_KEY,
    PROVIDER,
    make_boj_client,
)
from parsimony_boj.outputs import BOJ_FETCH_OUTPUT

logger = logging.getLogger(__name__)

# Frequency tokens emitted by BoJ. Anything not in the map passes through
# unchanged so an unknown frequency is never silently corrupted.
_FREQ_MAP: dict[str, str] = {
    "DAILY": "Daily",
    "DM": "Daily",
    "WEEKLY": "Weekly",
    "WEEKLY(MON)": "Weekly (Mon)",
    "WEEKLY(THU)": "Weekly (Thu)",
    "MONTHLY": "Monthly",
    "MM": "Monthly",
    "QUARTERLY": "Quarterly",
    "QM": "Quarterly",
    "SEMI-ANNUAL": "Semi-annual",
    "SEMIANNUAL": "Semi-annual",
    "SM": "Semi-annual",
    "ANNUAL": "Annual",
    "ANNUAL(MAR)": "Annual (Mar)",
    "AM": "Annual",
}


def _normalize_frequency(raw: str) -> str:
    """Normalize a BoJ frequency token to plain English title-case.

    Unknown tokens pass through unchanged so an unfamiliar value is never masked.
    """
    if not raw:
        return ""
    return _FREQ_MAP.get(raw.strip().upper(), raw)


def _parse_boj_date(date_str: str, freq: str) -> str:
    """Parse a BoJ survey-date token into an ISO ``YYYY-MM-DD`` string.

    Survey dates are compact integers whose width follows the series frequency
    (``19990101`` daily, ``199901`` monthly, ``199901`` quarter-of-year,
    ``1999`` annual). Unrecognised widths pass through unchanged so the
    downstream ``dtype="datetime"`` coercion can surface a real parse problem
    rather than us silently mangling the value.
    """
    freq_lower = freq.lower()
    if freq_lower in ("dm", "daily", "weekly"):
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    elif freq_lower in ("mm", "monthly"):
        if len(date_str) >= 6:
            return f"{date_str[:4]}-{date_str[4:6]}-01"
    elif freq_lower in ("qm", "quarterly"):
        if len(date_str) >= 6:
            quarter = int(date_str[4:6])
            month = (quarter - 1) * 3 + 1
            return f"{date_str[:4]}-{month:02d}-01"
    elif freq_lower in ("am", "annual", "sm", "semi-annual", "semiannual") and len(date_str) >= 4:
        return f"{date_str[:4]}-01-01"
    return date_str


def _validate_codes(code: str) -> str:
    """Validate + normalise the comma-separated ``code`` argument.

    Returns the cleaned comma-joined string. Raises ``InvalidParameterError``
    for an empty list or more than ``MAX_CODES_PER_REQUEST`` codes.
    """
    codes = [s.strip() for s in code.split(",") if s.strip()]
    if not codes:
        raise InvalidParameterError(PROVIDER, "At least one series code required")
    if len(codes) > MAX_CODES_PER_REQUEST:
        raise InvalidParameterError(PROVIDER, f"Maximum {MAX_CODES_PER_REQUEST} codes per request")
    return ",".join(codes)


def _parse_page(result_set: list[Any], *, seen: set[str]) -> list[dict[str, Any]]:
    """Parse one ``getDataCode`` page into observation rows.

    ``seen`` tracks series codes already emitted on earlier pages; a series that
    reappears (it should not — truncation is at series boundaries) is skipped so
    pagination can never double-count.
    """
    rows: list[dict[str, Any]] = []
    for series in result_set:
        if not isinstance(series, dict):
            continue
        series_code = series.get("SERIES_CODE", "")
        if series_code in seen:
            continue
        seen.add(series_code)
        name = series.get("NAME_OF_TIME_SERIES", series.get("NAME_OF_TIME_SERIES_J", series_code))
        freq = (series.get("FREQUENCY") or "").lower()
        values_block = series.get("VALUES") or {}
        dates = values_block.get("SURVEY_DATES", []) if isinstance(values_block, dict) else []
        values = values_block.get("VALUES", []) if isinstance(values_block, dict) else []

        if isinstance(dates, (str, int)):
            dates = [dates]
        if isinstance(values, (str, int, float)):
            values = [values]

        for date_str, raw_value in zip(dates, values, strict=False):
            try:
                value = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                value = None
            if value is None:
                continue
            rows.append(
                {
                    "code": series_code,
                    "title": name,
                    "date": _parse_boj_date(str(date_str), freq),
                    "value": value,
                }
            )
    return rows


@connector(output=BOJ_FETCH_OUTPUT, tags=["macro", "jp"])
def boj_fetch(
    db: str,
    code: Annotated[str, "ns:boj"],
    start_date: str | None = None,
    end_date: str | None = None,
    lang: str = "en",
) -> pd.DataFrame:
    """Fetch Bank of Japan time series by database and series code(s).

    ``db`` is a BoJ statistics database code (e.g. ``FM08`` for FX rates,
    ``PR01`` for prices); ``code`` is one or more comma-separated series codes
    (max 250, e.g. ``FXERD01`` or ``FXERD01,FXERD04``). All codes in one request
    must share the same frequency. ``start_date`` / ``end_date`` are period
    strings whose format follows the series frequency (e.g. ``YYYYMM`` for
    monthly/daily, ``YYYYQQ`` for quarterly). Returns one row per observation
    with ``code``, ``title``, ``date``, ``value``. Large multi-series requests
    that exceed the API's 60,000-point cap are transparently paginated, so the
    full result is always returned.
    """
    db_clean = db.strip().upper()
    if not db_clean:
        raise InvalidParameterError(PROVIDER, "db must be non-empty")
    codes = _validate_codes(code)

    client = make_boj_client()
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    start_position: int | None = None

    for _page in range(MAX_FETCH_PAGES):
        req_params: dict[str, Any] = {
            "db": db_clean,
            "code": codes,
            "lang": lang,
            "startDate": start_date or None,
            "endDate": end_date or None,
            "startPosition": start_position,
        }
        body = fetch_json(
            client,
            path="getDataCode",
            params=req_params,
            op_name="series",
        )
        if not isinstance(body, dict):
            raise ParseError(PROVIDER, f"unexpected response shape for db={db_clean}, code={codes}")

        result_set = body.get("RESULTSET")
        if result_set is None:
            result_set = []
        if not isinstance(result_set, list):
            raise ParseError(PROVIDER, f"RESULTSET is not a list for db={db_clean}, code={codes}")

        rows.extend(_parse_page(result_set, seen=seen))

        next_position = body.get(NEXTPOSITION_KEY)
        if not next_position:
            break
        try:
            advanced = int(next_position)
        except (TypeError, ValueError):
            break
        # Resume from NEXTPOSITION; bail if the cursor fails to advance (a
        # single series larger than the 60,000-point cap would do this — no BoJ
        # series is, but the guard keeps the loop finite regardless).
        if start_position is not None and advanced <= start_position:
            logger.warning(
                "boj_fetch: NEXTPOSITION did not advance (%s <= %s) for db=%s; stopping pagination",
                advanced,
                start_position,
                db_clean,
            )
            break
        start_position = advanced

    if not rows:
        raise EmptyDataError(
            PROVIDER,
            message=f"No data returned for db={db_clean}, code={codes}",
            query_params={"db": db_clean, "code": codes},
        )

    return pd.DataFrame(rows)


__all__ = ["boj_fetch"]
