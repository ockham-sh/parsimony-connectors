"""BdE series fetch connector."""

from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError
from parsimony.transport import map_http_error

from parsimony_bde._http import BASE_URL
from parsimony_bde.outputs import BDE_FETCH_OUTPUT
from parsimony_bde.params import BdeFetchParams


def _parse_bde_response(json_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Parse BdE JSON response into a long-format DataFrame."""
    all_rows: list[dict[str, Any]] = []

    for series in json_data:
        key = series.get("serie", "")
        title = series.get("descripcionCorta", series.get("descripcion", key))
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

            all_rows.append(
                {
                    "key": key,
                    "title": title,
                    "date": date_val,
                    "value": value,
                }
            )

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=["key", "title", "date", "value"])


@connector(output=BDE_FETCH_OUTPUT, tags=["macro", "es"])
async def bde_fetch(
    key: Annotated[str, "ns:bde"],
    time_range: str | None = None,
    lang: str = "en",
) -> pd.DataFrame:
    """Fetch Banco de España time series by series code(s)."""
    params = BdeFetchParams(key=key, time_range=time_range, lang=lang)
    url = f"{BASE_URL}/listaSeries"

    keys = [k.strip() for k in params.key.split(",") if k.strip()]
    json_data: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        for series_key in keys:
            req_params: dict[str, str] = {
                "idioma": params.lang,
                "series": series_key,
            }
            if params.time_range is not None:
                req_params["rango"] = str(params.time_range)

            response = await client.get(url, params=req_params)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                map_http_error(exc, provider="bde", op_name="series")
            data = response.json()
            if isinstance(data, list):
                json_data.extend(data)

    if not isinstance(json_data, list) or not json_data:
        raise EmptyDataError(provider="bde", message=f"BdE returned empty or invalid response for: {params.key}")

    df = _parse_bde_response(json_data)
    if df.empty:
        raise EmptyDataError(provider="bde", message=f"No observations parsed for: {params.key}")

    return df
