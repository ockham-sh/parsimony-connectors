"""SWEA family — interest & exchange rate series (the core Riksbank catalog).

Pure parsing + enumeration helpers for the SWEA product. SWEA exposes the whole
universe in two cheap calls: ``/Groups`` (the group hierarchy) and ``/Series``
(every series in one shot, no per-series fan-out). A series is fetched by id from
``/Observations/{id}/{from}/{to}`` (windowed) or ``/Observations/Latest/{id}``.

The ``/Observations`` payload carries only ``date``/``value`` — no title — so the
label is resolved with a secondary ``/Series`` request at fetch time.
"""

from __future__ import annotations

import re
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


_CURRENCY_SYNONYMS: dict[str, str] = {
    "EUR": "euro",
    "USD": "US dollar",
    "GBP": "British pound sterling",
    "JPY": "Japanese yen",
    "CHF": "Swiss franc",
    "NOK": "Norwegian krone",
    "DKK": "Danish krone",
}


def _counter_currency_label(ccy: str, series: dict[str, Any]) -> str:
    if ccy in _CURRENCY_SYNONYMS:
        return _CURRENCY_SYNONYMS[ccy]
    mid = (series.get("midDescription") or "").strip()
    if mid:
        return mid.rstrip(".")
    short = (series.get("shortDescription") or "").strip()
    return short or ccy


def _upstream_label_parts(series: dict[str, Any]) -> list[str]:
    """Distinct upstream SWEA label fields in short → mid → long order."""
    seen: set[str] = set()
    parts: list[str] = []
    for key in ("shortDescription", "midDescription", "longDescription"):
        v = series.get(key)
        if isinstance(v, str):
            text = v.strip()
            if text and text not in seen:
                seen.add(text)
                parts.append(text)
    return parts


_FX_GROUP_MARKERS: tuple[str, ...] = (
    "Currencies against Swedish kronor",
    "Cross rates",
)

_GVB_GROUP_MARKERS: tuple[str, ...] = (
    "Government Bonds",
    "SE GVB",
)

_FIXED_INCOME_GROUP_MARKERS: tuple[str, ...] = (
    "Treasury Bills",
    "SE TB",
    "Mortgage Bonds",
    "SE MB",
    "Commercial Paper",
    "SE CP",
    "Fixing Rates",
    "SE STFIX",
    "STIBOR",
    "Euribor",
    "Euro Market Rates",
    "Market rates",
)

# Country code → full name for international GVB series (e.g. USGVB5Y → "United States")
_GVB_COUNTRY_NAMES: dict[str, str] = {
    "US": "United States",
    "DE": "Germany",
    "JP": "Japan",
    "GB": "United Kingdom",
    "FR": "France",
    "NL": "Netherlands",
    "EM": "Euro area",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "SE": "Sweden",
}


def _is_fx_group(group_name: str) -> bool:
    return any(marker in group_name for marker in _FX_GROUP_MARKERS)


def _is_gvb_group(group_name: str) -> bool:
    return any(marker in group_name for marker in _GVB_GROUP_MARKERS)


def _is_fixed_income_group(group_name: str) -> bool:
    return any(marker in group_name for marker in _FIXED_INCOME_GROUP_MARKERS)


def _gvb_country_from_series_id(sid: str) -> str | None:
    """Extract two-letter country code from international GVB ids like ``USGVB5Y`` or ``DEGVB10Y``."""
    upper = sid.upper()
    if "GVB" not in upper:
        return None
    prefix = upper[: upper.index("GVB")]
    if len(prefix) == 2 and prefix.isalpha():
        return prefix
    return None


def _fx_currency_from_series_id(sid: str) -> str | None:
    """Extract counter-currency from SWEA FX ids like ``SEKEURPMI`` or ``SEKUSDPMM``."""
    upper = sid.upper()
    if not upper.startswith("SEK"):
        return None
    rest = upper[3:]
    for suffix in _FREQ_BY_SUFFIX:
        suffix_code = suffix[0]
        if rest.endswith(suffix_code):
            ccy = rest[: -len(suffix_code)]
            if 2 <= len(ccy) <= 4 and ccy.isalpha():
                return ccy
    return None


def series_description(series: dict[str, Any], sid: str, *, group_name: str = "", frequency: str = "") -> str:
    """Pick or compose searchable description text for a SWEA series row.

    FX / Government-Bond / fixed-income groups get a synthesised, keyword-rich
    description for retrieval; everything else falls back to the richest upstream
    label (long > mid > short), then an id + provider synthesis.
    """
    upstream = _upstream_label_parts(series)
    provider = series.get("source") or "Sveriges Riksbank"
    group_leaf = group_name.split(" > ")[-1].strip() if group_name else ""

    if _is_fx_group(group_name):
        ccy = _fx_currency_from_series_id(sid)
        parts: list[str] = []
        if ccy:
            ccy_label = _counter_currency_label(ccy, series)
            freq_hint = f"{frequency} " if frequency and frequency != "Unknown" else ""
            parts.append(
                f"{freq_hint}{ccy}/SEK exchange rate fixing — {ccy_label} against Swedish krona.".strip()
            )
        elif upstream:
            parts.append(upstream[-1])
        if group_leaf:
            parts.append(f"Group: {group_leaf}.")
        parts.append(f"Provider: {provider}.")
        if upstream:
            parts.append("Upstream labels: " + "; ".join(upstream) + ".")
        return " ".join(parts)

    if _is_gvb_group(group_name):
        country_code = _gvb_country_from_series_id(sid)
        country_name = _GVB_COUNTRY_NAMES.get(country_code or "", "") if country_code else ""
        # Use the most informative upstream label as the base, then inject "yield" keyword.
        base = ""
        for key in ("longDescription", "midDescription", "shortDescription"):
            v = series.get(key)
            if isinstance(v, str) and v.strip():
                base = v.strip()
                break
        if not base:
            base = f"Government bond, {sid}"
        parts_gvb: list[str] = [f"{base}."]
        if country_name and country_name not in base:
            parts_gvb.append(f"Country: {country_name}.")
        parts_gvb.append("Government bond interest rate yield.")
        return " ".join(parts_gvb)

    if _is_fixed_income_group(group_name):
        base = ""
        for key in ("longDescription", "midDescription", "shortDescription"):
            v = series.get(key)
            if isinstance(v, str) and v.strip():
                base = v.strip()
                break
        if base:
            return f"{base}. Interest rate."
        return f"{sid} — {group_leaf}. Interest rate."

    for key in ("longDescription", "midDescription", "shortDescription"):
        v = series.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return f"{sid} — published by {provider}."


def series_title(series: dict[str, Any], sid: str, *, group_name: str = "") -> str:
    """Resolve a human-readable title from a SWEA ``/Series`` entry.

    FX and Government-Bond groups get a composed, search-friendly title; otherwise
    prefer the short label, then mid/long description, finally the id.
    """
    short = (series.get("shortDescription") or "").strip()
    mid = (series.get("midDescription") or "").strip()
    long_desc = (series.get("longDescription") or "").strip()

    if _is_fx_group(group_name):
        ccy = _fx_currency_from_series_id(sid)
        if ccy:
            ccy_label = _counter_currency_label(ccy, series)
            return f"{ccy}/SEK exchange rate — {ccy_label} against Swedish krona"

    if _is_gvb_group(group_name):
        country_code = _gvb_country_from_series_id(sid)
        # For international GVBs like "US 5 Year", prefix with full country and "Government Bond"
        base_title = short or mid or long_desc or sid
        if country_code and country_code != "SE":
            country_name = _GVB_COUNTRY_NAMES.get(country_code, country_code)
            # Strip any leading 2–3 letter region token (US, JP, EU, DE, …) from the upstream label
            suffix = re.sub(r"^[A-Z]{2,3}\s+", "", base_title).strip() or base_title
            return f"{country_name} Government Bond {suffix} Yield".strip()
        # Swedish GVBs: "SE GVB 5 Year" → "Swedish Government Bond 5 Year Yield"
        base_title_upper = base_title.upper()
        suffix = base_title
        for prefix in ("SE GVB", "SEGVB", "SE STFIX"):
            if base_title_upper.startswith(prefix):
                suffix = base_title[len(prefix) :].strip()
                break
        return f"Swedish Government Bond {suffix} Yield".strip()

    title = short or mid or long_desc or sid
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
