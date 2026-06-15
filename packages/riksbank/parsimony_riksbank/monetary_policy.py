"""Monetary Policy Data family — the forecasts & outcomes behind the policy report.

The Riksbank publishes, with each Monetary Policy Report, a vintage of forecasts for a
fixed set of macro series (GDP, CPI/CPIF, unemployment, the policy rate, etc.). Each
publication is a **policy round** (e.g. ``2026:1``); a series fetched for a round
returns that round's full vintage — realised history up to the forecast cutoff plus the
forecast horizon.

Enumeration (``/forecasts/series_ids``) lists the ~24 series with rich metadata; the
data endpoint (``/forecasts?series=<id>&policy_round_name=<round>``) returns the
vintages. Omitting the round returns *every* vintage for the series.

The series-id scheme is ``COUNTRY-FREQUENCY-AREA-DECOMPOSITION-UNIT-ADJUSTED`` (e.g.
``SEQGDPNAYSA``); the third character encodes frequency (``Q``/``M``/``A``).
"""

from __future__ import annotations

from typing import Any

from parsimony_riksbank.swea import to_value

#: Code prefix that routes a catalog hit to the monetary-policy fetch verb. The bare
#: series ids overlap SWEA's id-prefix space (both have ``SED*``/``SEM*``/``SEA*``
#: forms), so the catalog code is prefixed to disambiguate routing.
CODE_PREFIX = "monetary_policy"

_GROUP = "Monetary Policy — forecasts & outcomes"

_FREQ_BY_CHAR: dict[str, str] = {"Q": "Quarterly", "M": "Monthly", "A": "Annual", "D": "Daily"}


def frequency_from_id(series_id: str) -> str:
    """Frequency from the 3rd character of the series id (``SE[Q|M|A]...``)."""
    sid = series_id.upper()
    if len(sid) >= 3 and sid[2] in _FREQ_BY_CHAR:
        return _FREQ_BY_CHAR[sid[2]]
    return "Unknown"


def _metadata_description(meta: dict[str, Any]) -> str:
    """Compose a searchable description from a series' metadata block.

    Folds the short ``description`` (e.g. "GDP"), the ``unit``, the ``source_agency``
    and any ``note`` into one prose blob so the catalog's text index has real signal —
    the bare two-word descriptions alone are too thin to discriminate.
    """
    desc = str(meta.get("description") or "").strip()
    unit = str(meta.get("unit") or "").strip()
    agency = str(meta.get("source_agency") or "").strip()
    note = str(meta.get("note") or "").strip()
    parts = [p for p in (desc, f"Unit: {unit}" if unit else "", f"Source: {agency}" if agency else "", note) if p]
    return ". ".join(parts) if parts else (desc or "Riksbank monetary policy forecast series.")


def build_monetary_policy_rows(series_payload: Any) -> list[dict[str, Any]]:
    """One catalog row per Monetary Policy series, from a ``/forecasts/series_ids`` body.

    The payload is ``{"data": [{"series_id", "metadata": {...}}, ...]}``. Each row gets
    a ``monetary_policy/<id>`` code (routing) and ``source="monetary_policy"``.
    """
    data = series_payload.get("data", []) if isinstance(series_payload, dict) else []
    rows: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        sid = entry.get("series_id")
        if not sid:
            continue
        meta = entry.get("metadata") or {}
        desc = str(meta.get("description") or sid).strip()
        unit = str(meta.get("unit") or "").strip()
        title = f"{desc} ({unit})" if unit else desc
        rows.append(
            {
                "code": f"{CODE_PREFIX}/{sid}",
                "title": title,
                "description": _metadata_description(meta),
                "source": "monetary_policy",
                "frequency": frequency_from_id(sid),
                "unit": unit,
                "group": _GROUP,
                "provider": str(meta.get("source_agency") or "Sveriges Riksbank"),
                "observation_min": str(meta.get("start_date") or "")[:10],
                "observation_max": "",  # Forecast horizon — no fixed end.
                "series_closed": False,
            }
        )
    return rows


def _vintage_rows(series_id: str, vintage: Any) -> list[dict[str, Any]]:
    """Flatten one vintage ``{metadata, observations}`` into observation rows."""
    if not isinstance(vintage, dict):
        return []
    vmeta = vintage.get("metadata") or {}
    policy_round = vmeta.get("policy_round") or ""
    cutoff = vmeta.get("forecast_cutoff_date") or ""
    rows: list[dict[str, Any]] = []
    for obs in vintage.get("observations", []):
        if not isinstance(obs, dict):
            continue
        dt = obs.get("dt") or obs.get("date")
        if dt is None:
            continue
        rows.append(
            {
                "series": series_id,
                "date": dt,
                "value": to_value(obs.get("value")),
                "policy_round": policy_round,
                "forecast_cutoff_date": cutoff,
            }
        )
    return rows


def parse_forecast_rows(payload: Any) -> tuple[list[dict[str, Any]], str]:
    """Flatten a ``/forecasts`` response into observation rows + resolve the title.

    Returns ``(rows, title)``. ``vintages`` is a single dict when ``policy_round_name``
    is supplied and a *list* of vintages when it is omitted (every published round); both
    shapes flatten through :func:`_vintage_rows`, with a ``policy_round`` column
    disambiguating the vintages. The title comes from the entry's own metadata so no
    secondary lookup is needed.
    """
    data = payload.get("data", []) if isinstance(payload, dict) else []
    rows: list[dict[str, Any]] = []
    title = ""
    for entry in data:
        if not isinstance(entry, dict):
            continue
        sid = entry.get("external_id") or entry.get("series_id") or ""
        meta = entry.get("metadata") or {}
        if not title:
            desc = str(meta.get("description") or sid).strip()
            unit = str(meta.get("unit") or "").strip()
            title = f"{desc} ({unit})" if unit else (desc or sid)
        vintages = entry.get("vintages")
        vintage_list = vintages if isinstance(vintages, list) else [vintages]
        for vintage in vintage_list:
            rows.extend(_vintage_rows(sid, vintage))
    return rows, title
