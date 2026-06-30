"""World Bank indicator fetch connector.

The World Bank API v2 provides development indicators (GDP, population, etc.)
per country and year. The API is **keyless** — no registration required.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json, make_http_client

from parsimony_worldbank._http import BASE_URL, DEFAULT_PAGE_SIZE, HEADERS, MAX_PAGES
from parsimony_worldbank.outputs import FETCH_COLUMNS, WORLDBANK_FETCH_OUTPUT

logger = logging.getLogger(__name__)


def _date_to_int(date_str: str | None) -> int | None:
    """Convert an ISO date string to a year integer, or pass None through."""
    if date_str is None:
        return None
    v = date_str.strip()
    if v.isdigit() and len(v) == 4:
        return int(v)
    raise InvalidParameterError("worldbank", f"date must be a 4-digit year (YYYY), got {date_str!r}")


@connector(output=WORLDBANK_FETCH_OUTPUT, tags=["macro", "international", "development"])
def worldbank_fetch(
    indicator_id: Annotated[str, "ns:worldbank"],
    country: str = "all",
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """Fetch World Bank development indicator data.

    Parameters
    ----------
    indicator_id:
        World Bank indicator series code (e.g. ``NY.GDP.MKTP.CD`` for GDP
        current US$). Discover codes via ``worldbank_search``.
    country:
        Country code (ISO 3166-1 alpha-3, or ``all`` for every country with
        data). Defaults to ``all``.
    date_from, date_to:
        Optional year bounds as ``YYYY`` (e.g. ``2010``). Pass ``None`` for no
        bound.

    Returns
    -------
    pd.DataFrame
        One row per (country, year) with columns ``indicator_id``,
        ``indicator_name``, ``country``, ``country_iso3``, ``date``, ``value``.
    """
    indicator_id = indicator_id.strip()
    if not indicator_id:
        raise InvalidParameterError("worldbank", "indicator_id must be non-empty")

    country = country.strip() if country else "all"
    year_from = _date_to_int(date_from) if date_from else None
    year_to = _date_to_int(date_to) if date_to else None

    # Build date range: World Bank API accepts e.g. "2010:2020" or "2010" or "all"
    date_param: str | None = None
    if year_from is not None and year_to is not None:
        date_param = f"{year_from}:{year_to}"
    elif year_from is not None:
        date_param = str(year_from)
    elif year_to is not None:
        date_param = str(year_to)

    path = f"/country/{country}/indicator/{indicator_id}"
    params: dict[str, Any] = {
        "format": "json",
        "per_page": DEFAULT_PAGE_SIZE,
    }
    if date_param:
        params["date"] = date_param

    client = make_http_client(BASE_URL, headers=HEADERS, timeout=30.0)

    all_rows: list[dict[str, Any]] = []
    page = 1
    while page <= MAX_PAGES:
        params["page"] = page
        data = fetch_json(
            client,
            path=path,
            params=params,
            provider="worldbank",
            op_name=f"observations_page_{page}",
        )

        if not isinstance(data, list) or len(data) < 2:
            break

        records = data[1]
        if not isinstance(records, list) or not records:
            break

        for record in records:
            if not isinstance(record, dict):
                continue
            raw_value = record.get("value")
            try:
                parsed_value = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                parsed_value = None

            indicator_info = record.get("indicator") or {}
            country_info = record.get("country") or {}

            all_rows.append(
                {
                    "indicator_id": indicator_id,
                    "indicator_name": str(indicator_info.get("value", "")),
                    "country": str(country_info.get("value", "")),
                    "country_iso3": str(record.get("countryiso3code", "")),
                    "date": str(record.get("date", "")),
                    "value": parsed_value,
                }
            )

        # Check if there are more pages
        meta = data[0]
        if isinstance(meta, dict):
            total = meta.get("total", 0)
            per_page = meta.get("per_page", DEFAULT_PAGE_SIZE)
            if isinstance(total, (int, float)) and isinstance(per_page, (int, float)):
                total_pages = -(-int(total) // int(per_page))  # ceil division
                if page >= total_pages:
                    break

        page += 1

    if not all_rows:
        raise EmptyDataError(
            "worldbank",
            message=f"No data for indicator={indicator_id}, country={country}",
            query_params={
                "indicator_id": indicator_id,
                "country": country,
            },
        )

    return pd.DataFrame(all_rows, columns=list(FETCH_COLUMNS))
