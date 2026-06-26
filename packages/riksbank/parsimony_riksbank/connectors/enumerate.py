"""Unified Riksbank enumerator — one row per addressable unit across five products.

The catalog universe is the union of:

* **SWEA** — live ``/Groups`` + ``/Series`` (the full ~117-series payload in one call).
* **SWESTR** — the static 7-series registry (separate URL family, not in ``/Series``).
* **Monetary Policy** — live ``/forecasts/series_ids`` (~24 forecast/outcome series).
* **Turnover** — the static 6-dataset registry (market x frequency).
* **Holdings** — the static 2-dataset registry.

The two live calls are bounded (no per-series fan-out); the three registries are I/O-free.
``_list_swea`` and ``_list_monetary_policy_series`` are the monkeypatch seams the live
integration suite trims so it never cold-builds the whole universe under the keyless quota.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from parsimony.connector import enumerator
from parsimony.errors import ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_riksbank import _http, holdings, monetary_policy, swea, swestr, turnover
from parsimony_riksbank.outputs import ENUMERATE_COLUMNS, RIKSBANK_ENUMERATE_OUTPUT


def _list_swea(api_key: str) -> tuple[Any, Any]:
    """Live SWEA discovery: ``/Groups`` (hierarchy) + ``/Series`` (every series)."""
    http = _http.swea_client(api_key)
    groups_data = fetch_json(http, path="Groups", provider="riksbank", op_name="Groups")
    series_data = fetch_json(http, path="Series", provider="riksbank", op_name="Series")
    return groups_data, series_data


def _list_monetary_policy_series(api_key: str) -> Any:
    """Live Monetary Policy discovery: ``/forecasts/series_ids``."""
    http = _http.monetary_policy_client(api_key)
    return fetch_json(http, path="forecasts/series_ids", provider="riksbank", op_name="series_ids")


def _swea_rows(groups_data: Any, series_data: Any) -> list[dict[str, Any]]:
    group_lookup = swea.flatten_groups(groups_data)
    if isinstance(series_data, dict):
        series_data = [series_data]
    if not isinstance(series_data, list):
        raise ParseError("riksbank", "unexpected /Series response shape (expected a list)")

    rows: list[dict[str, Any]] = []
    for s in series_data:
        if not isinstance(s, dict):
            continue
        sid = s.get("seriesId", "")
        if not sid:
            continue
        group_id_raw = s.get("groupId", "")
        try:
            group_id_int = int(group_id_raw) if group_id_raw != "" else None
        except (TypeError, ValueError):
            group_id_int = None
        group_name = group_lookup.get(str(group_id_raw), "")
        frequency = swea.infer_frequency(sid, group_id_int)
        rows.append(
            {
                "code": sid,
                "title": swea.series_title(s, sid, group_name=group_name),
                "description": swea.series_description(s, sid, group_name=group_name, frequency=frequency),
                "source": "swea",
                "frequency": frequency,
                "unit": "",
                "group": group_name,
                "provider": s.get("source", "") or "",
                "observation_min": swea.normalize_observation_date(s.get("observationMinDate")),
                "observation_max": swea.normalize_observation_date(s.get("observationMaxDate")),
                "series_closed": bool(s.get("seriesClosed", False)),
            }
        )
    return rows


@enumerator(output=RIKSBANK_ENUMERATE_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def enumerate_riksbank(api_key: str = "") -> pd.DataFrame:
    """Enumerate every addressable Riksbank unit across all five products.

    Two bounded live calls (SWEA ``/Groups`` + ``/Series``; Monetary Policy
    ``/forecasts/series_ids``) plus three static registries (SWESTR, Turnover, Holdings).
    SWEA is keyless; the optional ``api_key`` raises the quota.
    """
    groups_data, series_data = _list_swea(api_key)
    mp_payload = _list_monetary_policy_series(api_key)

    rows: list[dict[str, Any]] = []
    rows.extend(_swea_rows(groups_data, series_data))
    rows.extend(swestr.build_swestr_rows())
    rows.extend(monetary_policy.build_monetary_policy_rows(mp_payload))
    rows.extend(turnover.build_turnover_rows())
    rows.extend(holdings.build_holdings_rows())

    columns = list(ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
