"""BdP series fetch connector.

Banco de Portugal BPstat is a **keyless** public JSON API — no api_key, no
``secrets=``/``bind()``/``load()``, no ``UnauthorizedError``. The dataset-detail
endpoint returns a JSON-stat 2.0 envelope; we melt its row-major ``value`` array
(series × dates) into one row per (series, observation).
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json, make_http_client

from parsimony_bdp._http import BASE_URL, HEADERS, VALID_LANGS
from parsimony_bdp.outputs import BDP_FETCH_OUTPUT, FETCH_COLUMNS

logger = logging.getLogger(__name__)


def _validate_period(name: str, value: str | None) -> str | None:
    """Screen an ``obs_since`` / ``obs_to`` argument as an ISO ``YYYY-MM-DD`` date.

    Rejected pre-network with :class:`InvalidParameterError`; ``None`` passes
    through as "no bound".
    """
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    parts = v.split("-")
    if len(parts) == 3 and parts[0].isdigit() and len(parts[0]) == 4 and parts[1].isdigit() and parts[2].isdigit():
        return v
    raise InvalidParameterError("bdp", f"{name} must be an ISO date (YYYY-MM-DD), got {value!r}")


def _parse_dataset_observations(json_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Melt a JSON-stat 2.0 dataset-detail payload into long observation rows.

    Returns one ``{series_id, title, date, value}`` row per (series, date). The
    ``value`` array is row-major over (series × dates); ``extension.series``
    supplies each series' id + label, with a positional fallback (logged) when
    the value array implies more series than the metadata declared.
    """
    role = json_data.get("role", {})
    time_dims = role.get("time", []) if isinstance(role, dict) else []
    time_dim_key = time_dims[0] if time_dims else None

    dimension = json_data.get("dimension", {})
    dates: list[str] = []
    if time_dim_key and isinstance(dimension, dict) and time_dim_key in dimension:
        cat = dimension[time_dim_key].get("category", {})
        index = cat.get("index", {})
        if isinstance(index, dict):
            dates = list(index.keys())
        elif isinstance(index, list):
            dates = [str(d) for d in index]

    raw_values = json_data.get("value", [])
    if isinstance(raw_values, dict):
        values_list: list[Any] = (
            [raw_values.get(str(i)) for i in range(max(int(k) for k in raw_values) + 1)] if raw_values else []
        )
    else:
        values_list = list(raw_values)

    if not dates or not values_list:
        return []

    series_info = json_data.get("extension", {}).get("series", [])
    if not isinstance(series_info, list):
        series_info = []
    n_dates = len(dates)
    n_series = len(values_list) // n_dates if n_dates else 1

    rows: list[dict[str, Any]] = []
    for s_idx in range(n_series):
        if s_idx >= len(series_info) or not isinstance(series_info[s_idx], dict):
            logger.warning(
                "BdP series index %d exceeds extension.series length %d; positional id fallback",
                s_idx,
                len(series_info),
            )
            sid = str(s_idx)
            label = sid
        else:
            sid = str(series_info[s_idx].get("id", s_idx))
            label = str(series_info[s_idx].get("label", sid))

        for d_idx, date_str in enumerate(dates):
            val_idx = s_idx * n_dates + d_idx
            if val_idx >= len(values_list):
                break
            raw = values_list[val_idx]
            try:
                value = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                value = None
            rows.append({"series_id": sid, "title": label, "date": date_str, "value": value})

    return rows


@connector(output=BDP_FETCH_OUTPUT, tags=["macro", "pt"])
def bdp_fetch(
    domain_id: int,
    dataset_id: Annotated[str, "ns:bdp"],
    series_ids: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    lang: str = "en",
) -> pd.DataFrame:
    """Fetch Banco de Portugal time series by domain ID + dataset ID.

    Uses the BPstat JSON-stat API. Discover ``domain_id``/``dataset_id`` pairs
    (and the optional per-series ``series_ids`` filter) via ``bdp_search`` /
    ``enumerate_bdp`` — a search hit's compound code ``domain:dataset:series``
    splits straight into these arguments. ``start_date``/``end_date``
    (``YYYY-MM-DD``) bound the observation window; ``lang`` selects the label
    language (``en`` or ``pt``). Returns one row per observation with
    ``series_id``, ``title``, ``date``, ``value``.
    """
    dataset_id = dataset_id.strip()
    if not dataset_id:
        raise InvalidParameterError("bdp", "dataset_id must be non-empty")
    lang_norm = lang.strip().lower()
    if lang_norm not in VALID_LANGS:
        raise InvalidParameterError("bdp", "lang must be 'en' or 'pt'")
    obs_since = _validate_period("start_date", start_date)
    obs_to = _validate_period("end_date", end_date)

    req_params: dict[str, Any] = {
        "lang": lang_norm.upper(),
        "series_ids": series_ids.strip() if series_ids else None,
        "obs_since": obs_since,
        "obs_to": obs_to,
    }
    json_data = fetch_json(
        make_http_client(BASE_URL, headers=HEADERS, timeout=60.0),
        path=f"domains/{domain_id}/datasets/{dataset_id}/",
        params=req_params,
        provider="bdp",
        op_name="observations",
    )

    if not isinstance(json_data, dict):
        raise ParseError("bdp", f"unexpected response shape for domain={domain_id}, dataset={dataset_id}")

    rows = _parse_dataset_observations(json_data)
    if not rows:
        raise EmptyDataError(
            "bdp",
            message=f"No observations for domain={domain_id}, dataset={dataset_id}",
            query_params={
                "domain_id": domain_id,
                "dataset_id": dataset_id,
                "series_ids": series_ids,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

    return pd.DataFrame(rows, columns=list(FETCH_COLUMNS))
