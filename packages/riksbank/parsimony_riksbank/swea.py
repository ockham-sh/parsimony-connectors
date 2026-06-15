"""SWEA family — interest & exchange rate series (the core Riksbank catalog).

Pure parsing + enumeration helpers for the SWEA product. SWEA exposes the whole
universe in two cheap calls: ``/Groups`` (the group hierarchy) and ``/Series``
(every series in one shot, no per-series fan-out). A series is fetched by id from
``/Observations/{id}/{from}/{to}`` (windowed) or ``/Observations/Latest/{id}``.

The ``/Observations`` payload carries only ``date``/``value`` — no title — so the
label is resolved with a secondary ``/Series`` request at fetch time.
"""

from __future__ import annotations

from datetime import date
from typing import Any

# Suffix -> frequency mapping. Riksbank's series-id convention is undocumented but
# the suffix letter is stable: ``PMI``/``PMD`` daily, ``PMW``/``PMM`` weekly/monthly,
# ``PMQ`` quarterly, ``PMA`` annual.
_FREQ_BY_SUFFIX: tuple[tuple[str, str], ...] = (
    ("PMI", "Daily"),
    ("PMD", "Daily"),
    ("PMW", "Weekly"),
    ("PMM", "Monthly"),
    ("PMQ", "Quarterly"),
    ("PMA", "Annual"),
)

# Group-id -> frequency for buckets where the SWEA hierarchy pins the cadence.
# Interest-rate and FX series publish daily at 16:15 CET; monthly/annual aggregate
# buckets carry their own group ids (133, 134).
_FREQ_BY_GROUP_ID: dict[int, str] = {
    2: "Daily",  # Riksbank key interest rates
    3: "Daily",  # Other Riksbank interest rates
    5: "Daily",  # STIBOR
    6: "Daily",  # Swedish Treasury Bills (SE TB)
    7: "Daily",  # Swedish Government Bonds (SE GVB)
    8: "Daily",  # Swedish Fixing Rates (SE STFIX)
    9: "Daily",  # Swedish Mortgage Bonds (SE MB)
    10: "Daily",  # Swedish Commercial Paper (SE CP)
    97: "Daily",  # Euro Market Rates, 3 months
    98: "Daily",  # Euro Market Rates, 6 months
    99: "Daily",  # International Government Bonds, 5 years
    100: "Daily",  # International Government Bonds, 10 years
    12: "Daily",  # Swedish TCW index
    130: "Daily",  # Currencies against Swedish kronor
    131: "Daily",  # Cross rates
    138: "Daily",  # Special Drawing Rights (SDR)
    151: "Daily",  # Swedish KIX index
    155: "Daily",  # Forward Premiums
    133: "Monthly",  # Monthly aggregate
    134: "Annual",  # Annual aggregate
}


def infer_frequency(series_id: str, group_id: int | None) -> str:
    """Best-effort frequency for a SWEA series — group cadence beats id suffix."""
    if group_id is not None and group_id in _FREQ_BY_GROUP_ID:
        return _FREQ_BY_GROUP_ID[group_id]
    sid = series_id.upper()
    for suffix, freq in _FREQ_BY_SUFFIX:
        if sid.endswith(suffix):
            return freq
    return "Unknown"


def flatten_groups(root: Any) -> dict[str, str]:
    """Walk the SWEA ``/Groups`` tree into ``{group_id: full_path_name}``.

    The endpoint returns a single root node with ``childGroups``; each node carries
    ``groupId``, ``name``, ``description``. Legacy keys are accepted defensively in
    case the API surface ever changes.
    """
    lookup: dict[str, str] = {}

    def _walk(node: Any, parent: str = "") -> None:
        if isinstance(node, list):
            for item in node:
                _walk(item, parent)
            return
        if not isinstance(node, dict):
            return
        gid = node.get("groupId", node.get("id", ""))
        name = node.get("name", node.get("groupName", ""))
        full = f"{parent} > {name}" if parent and name else (name or parent)
        if gid != "":
            lookup[str(gid)] = full
        children = node.get("childGroups") or node.get("groupInfos") or node.get("children") or []
        _walk(children, full)

    _walk(root)
    return lookup


def series_description(series: dict[str, Any]) -> str:
    """Pick the richest description SWEA offers for a series.

    Preference: ``longDescription`` (full sentences) > ``midDescription`` >
    ``shortDescription`` (label). A fall-back synthesised from id + provider keeps
    the DESCRIPTION column non-empty.
    """
    for key in ("longDescription", "midDescription", "shortDescription"):
        v = series.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    sid = series.get("seriesId") or "series"
    provider = series.get("source") or "Sveriges Riksbank"
    return f"{sid} — published by {provider}."


def series_title(series: dict[str, Any], sid: str) -> str:
    """Resolve a human-readable title from a SWEA ``/Series`` entry.

    SWEA rows carry ``shortDescription`` (the UI label) and the longer description
    fields — there is no ``seriesName``/``name`` key despite older code looking for
    them. Prefer the short label, then the mid description, finally the id.
    """
    title = series.get("shortDescription") or series.get("midDescription") or sid
    return str(title).strip() or sid


def normalize_observation_date(value: Any) -> str:
    """Coerce an upstream date-like field to ``YYYY-MM-DD`` or ``""``."""
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return ""
    return s[:10]


def to_value(raw_value: Any) -> float | None:
    """Coerce a raw rate/index field to float, or ``None`` if absent/blank."""
    if raw_value in (None, "", "NaN"):
        return None
    try:
        return float(raw_value)
    except (ValueError, TypeError):
        return None


def parse_observations(series_id: str, title: str, payload: Any) -> list[dict[str, Any]]:
    """Flatten a SWEA ``/Observations`` response into ``{series_id, title, date, value}`` rows.

    ``/Observations/Latest/{id}`` returns a single object; the windowed endpoint
    returns a list. Tolerate both ``date``/``Date`` and ``value``/``Value`` casings.
    """
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        date_val = item.get("date") or item.get("Date")
        if date_val is None:
            continue
        raw_value = item.get("value")
        if raw_value is None:
            raw_value = item.get("Value")
        rows.append({"series_id": series_id, "title": title, "date": date_val, "value": to_value(raw_value)})
    return rows
