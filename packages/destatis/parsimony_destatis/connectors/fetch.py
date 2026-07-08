"""Destatis table fetch connector.

GENESIS-Online is a **keyless** public JSON-stat API — no api_key, no
``secrets=``/``bind()``/``load()``, no ``UnauthorizedError``. The
``/tables/{code}/data`` endpoint returns a JSON-stat 2.0 dataset (or a
``{"data": [...]}`` envelope of datasets) which we reshape into one row per
(series, observation).
"""

from __future__ import annotations

import json
import re
from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    RateLimitError,
)
from parsimony.transport import check_status

from parsimony_destatis._http import looks_like_html, make_client
from parsimony_destatis.outputs import DESTATIS_FETCH_OUTPUT

_GERMAN_MONTHS = {
    "Januar": "01",
    "Februar": "02",
    "März": "03",
    "April": "04",
    "Mai": "05",
    "Juni": "06",
    "Juli": "07",
    "August": "08",
    "September": "09",
    "Oktober": "10",
    "November": "11",
    "Dezember": "12",
}

# Markers GENESIS uses in a 200-with-error body to signal anonymous-access
# throttling (the daily/per-window request budget). String-sniffing here is the
# §5.8 carve-out: GENESIS exposes no machine-readable code in the 200 body, so a
# quota/throttle phrase → RateLimitError, every other non-data 200 → ParseError.
_RATE_LIMIT_MARKERS = (
    "zu viele",  # "too many requests" (DE)
    "too many",
    "request limit",
    "kontingent",  # quota (DE)
    "ausgeschöpft",  # exhausted (DE)
)


def _normalize_german_date(s: str) -> str:
    """Best-effort coercion of a German period label to ISO ``YYYY-MM-DD``."""
    s = s.strip()
    if re.match(r"^\d{4}$", s):
        return f"{s}-01-01"
    q_match = re.match(r"(\d)\.\s*Quartal\s+(\d{4})", s)
    if q_match:
        quarter = int(q_match.group(1))
        month = (quarter - 1) * 3 + 1
        return f"{q_match.group(2)}-{month:02d}-01"
    for month_de, month_num in _GERMAN_MONTHS.items():
        if month_de in s:
            year_match = re.search(r"(\d{4})", s)
            if year_match:
                return f"{year_match.group(1)}-{month_num}-01"
    if re.match(r"^\d{4}-\d{2}$", s):
        return f"{s}-01"
    return s


# GENESIS encodes the time axis as a dimension whose *category index keys* are
# the period values themselves (the ``label`` is usually null). Those keys are
# ISO-ish and frequency-dependent:
#   ``2012``         year (JAHR)              ``2026-01``    year-month
#   ``1999-12-31``   reference date (STAG/STAGV)
#   ``2015-05P1M``   month, ISO-8601 duration (SMONAT)
#   ``2015-04P3M``   quarter (SQUART)         ``2003-10P6M``  semester (SEMEST)
#   ``2000-P1Y``     year / school-year (SLJAHR)
# Crucially, name-lookalike dims are *classifications*, not the time axis:
# ``MONAT`` keys are ``MONAT10`` (month-of-year) and ``QUARTG`` keys are
# ``QUART3`` (quarter-of-year) — neither matches a period pattern, so they are
# correctly excluded.
_ISO_DURATION_RE = re.compile(r"^(\d{4})(?:-(\d{2}))?-?P\d+[DWMY]$")
_PERIOD_RES = (
    re.compile(r"^\d{4}$"),
    re.compile(r"^\d{4}-\d{2}$"),
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    _ISO_DURATION_RE,
)


def _looks_like_period(key: str) -> bool:
    """Does a category-index key look like a time period (vs a classification)?"""
    return any(p.match(key) for p in _PERIOD_RES)


def _normalize_period(token: str) -> str:
    """Coerce a GENESIS time-dimension key/label to the ISO ``YYYY-MM-DD`` period start."""
    s = token.strip()
    m = _ISO_DURATION_RE.match(s)
    if m:
        year, month = m.group(1), m.group(2)
        return f"{year}-{month or '01'}-01"
    if re.match(r"^\d{4}$", s):
        return f"{s}-01-01"
    if re.match(r"^\d{4}-\d{2}$", s):
        return f"{s}-01"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # Legacy/defensive: the live API no longer sends German month/quarter labels
    # at the table-data level, but tolerate them if a future table does.
    return _normalize_german_date(s)


def _parse_jsonstat(payload: dict[str, Any], table_code: str) -> pd.DataFrame:
    """Parse a JSON-stat 2.0 dataset into a long-format DataFrame."""
    label = str(payload.get("label") or table_code)

    dim_ids = payload.get("id") or []
    sizes = payload.get("size") or []
    dimensions = payload.get("dimension") or {}
    raw_values = payload.get("value")

    if not isinstance(dim_ids, list) or not isinstance(sizes, list):
        raise ParseError("destatis", f"JSON-stat payload for {table_code} missing id/size arrays")
    if len(dim_ids) != len(sizes):
        raise ParseError("destatis", f"JSON-stat id/size length mismatch for {table_code}")

    total = 1
    for s in sizes:
        total *= int(s)

    if isinstance(raw_values, list):
        values_flat: list[Any] = list(raw_values)
        if len(values_flat) < total:
            values_flat.extend([None] * (total - len(values_flat)))
    elif isinstance(raw_values, dict):
        values_flat = [None] * total
        for k, v in raw_values.items():
            try:
                idx = int(k)
            except (TypeError, ValueError):
                continue
            if 0 <= idx < total:
                values_flat[idx] = v
    else:
        values_flat = [None] * total

    dim_indices: list[list[str]] = []
    dim_labels: list[dict[str, str]] = []
    for did in dim_ids:
        d = dimensions.get(did) or {}
        category = d.get("category") if isinstance(d, dict) else None
        if not isinstance(category, dict):
            dim_indices.append([])
            dim_labels.append({})
            continue
        index = category.get("index")
        if isinstance(index, list):
            ordered = [str(x) for x in index]
        elif isinstance(index, dict):
            ordered_pairs = sorted(((int(v), str(k)) for k, v in index.items()), key=lambda p: p[0])
            ordered = [k for _, k in ordered_pairs]
        else:
            ordered = []
        labels = category.get("label") or {}
        if not isinstance(labels, dict):
            labels = {}
        dim_indices.append(ordered)
        dim_labels.append({str(k): str(v) for k, v in labels.items()})

    # Identify the time dimension by the *shape of its category keys*, not its
    # name. GENESIS time dims (JAHR/STAG/STAGV/SEMEST/SMONAT/SQUART/SLJAHR) all
    # carry period-shaped keys; name-lookalikes (MONAT="MONAT10",
    # QUARTG="QUART3") are classifications. Pick the dimension whose keys are
    # most period-shaped (majority), preferring the last such dim (GENESIS
    # usually orders time last). NEVER fall back to dimension 0 — that is the
    # constant ``statistic`` dim whose key is the table's statistic code (the
    # old name-based code fell back to it and emitted that code as a bogus
    # "year", hard-failing every STAG/SEMEST/SMONAT/SQUART table).
    time_dim_idx: int | None = None
    best_fraction = 0.0
    for i in range(len(dim_ids)):
        keys = dim_indices[i]
        if not keys:
            continue
        fraction = sum(_looks_like_period(k) for k in keys) / len(keys)
        if fraction >= 0.5 and fraction >= best_fraction:
            best_fraction = fraction
            time_dim_idx = i

    rows: list[dict[str, Any]] = []
    for flat_idx in range(total):
        coord: list[int] = []
        rem = flat_idx
        for size in reversed(sizes):
            size_i = int(size)
            coord.append(rem % size_i if size_i > 0 else 0)
            rem //= max(size_i, 1)
        coord.reverse()

        raw_val = values_flat[flat_idx]
        try:
            value = float(raw_val) if raw_val is not None else None
        except (TypeError, ValueError):
            value = None
        if value is None:
            continue

        row: dict[str, Any] = {
            "series_id": table_code,
            "title": label,
            "value": value,
        }
        for dim_pos, cat_pos in enumerate(coord):
            dim_id = str(dim_ids[dim_pos])
            ordered = dim_indices[dim_pos]
            cat_key = ordered[cat_pos] if 0 <= cat_pos < len(ordered) else ""
            cat_label = dim_labels[dim_pos].get(cat_key, cat_key)
            if dim_pos == time_dim_idx:
                # The index key is the authoritative period (label is usually
                # null); prefer it over the label.
                row["date"] = _normalize_period(cat_key or cat_label)
            else:
                row[dim_id] = cat_label or cat_key

        if "date" not in row:
            row["date"] = ""
        rows.append(row)

    if not rows:
        raise EmptyDataError(
            "destatis",
            message=f"No observations parsed from JSON-stat for table {table_code}",
            query_params={"name": table_code},
        )

    df = pd.DataFrame(rows)

    role = payload.get("role") or {}
    if isinstance(role, dict):
        unit_dims = role.get("unit") or []
        if isinstance(unit_dims, list) and unit_dims:
            df["unit"] = ",".join(str(u) for u in unit_dims)

    return df


def _get_text(path: str, *, params: dict[str, str] | None = None, op_name: str) -> str:
    """GET ``path`` and return the raw text, mapping HTTP/timeout errors typed.

    The single-table data endpoint is JSON-stat, but we read it as text first so
    we can distinguish a real dataset from a 200-with-error body (HTML
    maintenance shell or a throttle notice) per §5.8 before handing it to the
    JSON parser. ``HttpClient.request`` never raises on status (transport
    failures are mapped internally); ``check_status`` raises the typed error
    from the status code for everything else.
    """
    http = make_client()
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    check_status(response, provider="destatis", op_name=op_name)
    return response.text


@connector(output=DESTATIS_FETCH_OUTPUT, tags=["macro", "de"])
def destatis_fetch(
    name: Annotated[str, Namespace("destatis")],
    start_year: str | None = None,
    end_year: str | None = None,
) -> pd.DataFrame:
    """Fetch a Destatis GENESIS table by table code (e.g. ``61111-0001``).

    Hits the public ``genesis.destatis.de/genesis/api/rest/tables/{code}/data``
    endpoint (anonymous, keyless) and parses the JSON-stat 2.0 response into a
    long-format DataFrame with one row per observation (series_id, title, date,
    value). ``start_year`` / ``end_year`` (4-digit years) bound the ``date``
    axis: they are forwarded to GENESIS *and* re-applied client-side, because
    GENESIS ignores the bound on some tables and returns the full span.

    Note: on ``JAHR`` (annual) tables the sub-annual axis (month/quarter) lives
    in a *classification* column (e.g. ``MONAT``/``QUARTG``), not in ``date`` —
    ``date`` carries the year; read the classification column for the finer grain.
    """
    table_code = name.strip()
    if not table_code:
        raise InvalidParameterError("destatis", "name (table code) must be non-empty")

    query: dict[str, str] = {}
    if start_year:
        query["startyear"] = start_year
    if end_year:
        query["endyear"] = end_year

    text = _get_text(f"/tables/{table_code}/data", params=query or None, op_name="data")

    # §5.8 — 200-with-error body. GENESIS can return HTTP 200 with the SPA /
    # maintenance HTML shell (host swap) or a throttle notice instead of a
    # JSON-stat dataset. Never fake a status; map by body shape.
    if looks_like_html(text):
        lowered = text.lower()
        if any(marker in lowered for marker in _RATE_LIMIT_MARKERS):
            raise RateLimitError(
                "destatis",
                retry_after=3600.0,
                quota_exhausted=True,
                message="GENESIS anonymous-access request budget exhausted",
            )
        raise ParseError(
            "destatis",
            f"GENESIS returned an HTML page instead of JSON-stat data for {table_code} "
            "(API host/path may have changed)",
        )

    try:
        payload = json.loads(text)
    except ValueError as exc:
        raise ParseError("destatis", f"Failed to parse JSON-stat for {table_code}: {exc}") from exc

    if not isinstance(payload, dict):
        raise ParseError("destatis", f"JSON-stat response for {table_code} was not an object")

    datasets: list[dict[str, Any]]
    raw_data = payload.get("data")
    if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict):
        datasets = [d for d in raw_data if isinstance(d, dict)]
    elif isinstance(raw_data, dict):
        datasets = [raw_data]
    elif "id" in payload and "value" in payload:
        datasets = [payload]
    else:
        raise ParseError(
            "destatis",
            f"Unexpected JSON-stat envelope for {table_code}: keys={list(payload.keys())}",
        )

    frames = [_parse_jsonstat(d, table_code) for d in datasets]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    # GENESIS honours ``startyear``/``endyear`` inconsistently (some tables return
    # the full span regardless), so enforce the window client-side. ``date`` is
    # normalized to ISO ``YYYY-MM-DD``, so lexical string comparison is a correct
    # date filter.
    if start_year:
        df = df[df["date"] >= f"{start_year}-01-01"]
    if end_year:
        df = df[df["date"] <= f"{end_year}-12-31"]
    if (start_year or end_year) and df.empty:
        raise EmptyDataError(
            "destatis",
            message=f"No observations in {start_year or '..'}–{end_year or '..'} for table {table_code}",
            query_params={"name": table_code, "start_year": start_year, "end_year": end_year},
        )
    return df.reset_index(drop=True)
