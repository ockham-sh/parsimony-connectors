"""World Bank Open Data connector for parsimony.

Provides access to the World Bank's development indicators via the
v2 API (``api.worldbank.org/v2``). The World Bank Indicators API is
**keyless and open** — no authentication required.

Exports:

* :data:`CONNECTORS` — the :class:`parsimony.Connectors` collection exposed
  via the ``parsimony.providers`` entry point. Includes ``worldbank_search``
  (tool-tagged for agent use) and ``worldbank_fetch``.
* :func:`load` — returns :data:`CONNECTORS` (no secrets to bind, since the
  API is keyless).
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
)
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient
from parsimony.transport.helpers import fetch_json, make_http_client

logger = logging.getLogger(__name__)

__all__ = ["CONNECTORS", "load"]

_BASE_URL = "https://api.worldbank.org/v2"

# Maximum rows per page. The World Bank API caps this at ~50 000. Keep a
# generous default below the limit to avoid silent truncation.
_PER_PAGE = 50_000

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="id", role=ColumnRole.KEY, namespace="worldbank"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="source_note", role=ColumnRole.METADATA),
        Column(name="source_organization", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="source_id", role=ColumnRole.METADATA),
    ]
)

_SEARCH_COLUMNS = [c.name for c in SEARCH_OUTPUT.columns]

FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="indicator", role=ColumnRole.KEY, namespace="worldbank"),
        Column(name="country", role=ColumnRole.TITLE),
        Column(name="countryiso3code", role=ColumnRole.METADATA),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="title", role=ColumnRole.METADATA),
    ]
)

_FETCH_COLUMNS = [c.name for c in FETCH_OUTPUT.columns]


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _client() -> HttpClient:
    """Build a World Bank API HTTP client.

    The API is keyless — no authentication headers or query params needed.
    The ``format=json`` param is added as a default query parameter on every
    request so callers don't need to repeat it.
    """
    return make_http_client(_BASE_URL, query_params={"format": "json"})


def _parse_wb_response(body: Any) -> list[dict[str, Any]]:
    """Parse the World Bank API's two-element array response.

    The World Bank v2 API returns ``[metadata_object, data_array]``. This
    helper unpacks the data array and raises structured errors for known
    failure modes:

    * **HTTP‐200‐with‐error** — some endpoints return ``[{'message': …}]``
      with a HTTP 200 status.
    * **Empty data** — ``[metadata, []]`` signals no matching records.
    * **Malformed payload** — anything that isn't the expected two-element
      tuple.
    """
    if not isinstance(body, list) or len(body) != 2:
        raise ParseError("worldbank", f"Expected a two-element array; got {type(body).__name__}")

    # The first element is pagination metadata; the second is the data array.
    records = body[1]

    if records is None:
        raise EmptyDataError("worldbank")

    if not isinstance(records, list):
        raise ParseError("worldbank", f"Expected data array in position 1; got {type(records).__name__}")

    # The API may return a single error message object instead of a record
    # array. Detect that by checking for a 'message' key.
    if len(records) == 1 and isinstance(records[0], dict) and "message" in records[0]:
        msg = records[0]["message"]
        raise ParseError("worldbank", f"API message: {msg}")

    if not records:
        raise EmptyDataError("worldbank")

    return records


def _coerce_value(row: dict[str, Any]) -> float | None:
    """Convert a World Bank ``value`` field (string or None) to a float.

    The API returns numeric values as strings when present and ``None`` when
    the observation is missing. Pandas will convert None to NaN naturally,
    but we ensure the output column is numeric.
    """
    v = row.get("value")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        logger.warning(
            "worldbank: non-numeric value %r for %s %s; coercing to NaN",
            v,
            row.get("indicator"),
            row.get("date"),
        )
        return None


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=SEARCH_OUTPUT, tags=["macro", "tool", "worldbank"])
def worldbank_search(query: str) -> pd.DataFrame:
    """Search World Bank development indicators by keyword.

    Searches the full indicator catalog (``/v2/indicator``) and returns
    matching indicators whose **id** or **name** contains the query text.
    Use short, specific queries like 'GDP' or 'population' or 'education'.

    The query is case-insensitive and matches substrings. Results are
    sorted by id in ascending order.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError("worldbank", "query must be non-empty")

    http = _client()
    body = fetch_json(
        http,
        path="indicator",
        params={"per_page": _PER_PAGE},
        provider="worldbank",
        op_name="indicator",
    )

    records = _parse_wb_response(body)

    # Case-insensitive substring filter on id and name.
    q_lower = q.lower()
    matched = [
        r
        for r in records
        if isinstance(r, dict)
        and r.get("id")
        and (
            q_lower in r["id"].lower() or q_lower in (r.get("name") or "").lower()
        )
    ]

    if not matched:
        raise EmptyDataError("worldbank", query_params={"query": q})

    df = pd.DataFrame(matched)
    # Normalise column names to match our schema:
    # - 'sourceNote' -> 'source_note'
    # - 'sourceOrganization' -> 'source_organization'
    # - 'source' is a nested dict; extract its id as 'source_id'
    df = df.rename(
        columns={
            "sourceNote": "source_note",
            "sourceOrganization": "source_organization",
        }
    )
    if "source" in df.columns:
        df["source_id"] = df["source"].apply(lambda s: s.get("id") if isinstance(s, dict) else None)
    cols = [c for c in _SEARCH_COLUMNS if c in df.columns]
    df = df[cols].sort_values("id").reset_index(drop=True)
    return df


@connector(output=FETCH_OUTPUT, tags=["macro", "worldbank"])
def worldbank_fetch(
    indicator: Annotated[str, Namespace("worldbank")],
    country: str = "all",
    date: str | None = None,
) -> pd.DataFrame:
    """Fetch World Bank development indicator data.

    Returns indicator observations for one or more countries. ``indicator``
    is a World Bank indicator code (e.g. ``SP.POP.TOTL`` for population,
    ``NY.GDP.MKTP.CD`` for GDP in current USD). ``country`` is an ISO 3166‑1
    alpha‑2 or alpha‑3 code, or ``all`` for every available economy
    (regional aggregates are also included). ``date`` filters by year;
    use a single year (``"2024"``) or a range (``"2020:2024"``).

    The API is keyless — no authentication required.
    """
    indicator = indicator.strip()
    if not indicator:
        raise InvalidParameterError("worldbank", "indicator must be non-empty")

    country = country.strip()
    if not country:
        raise InvalidParameterError("worldbank", "country must be non-empty")

    http = _client()
    path = f"country/{country}/indicator/{indicator}"
    params: dict[str, Any] = {"per_page": _PER_PAGE}
    if date:
        params["date"] = date

    body = fetch_json(
        http,
        path=path,
        params=params,
        provider="worldbank",
        op_name=f"country/{country}/indicator/{indicator}",
    )

    records = _parse_wb_response(body)

    rows = []
    for r in records:
        if not isinstance(r, dict):
            continue
        indicator_info = r.get("indicator") or {}
        country_info = r.get("country") or {}
        rows.append(
            {
                "indicator": indicator_info.get("id") or indicator,
                "country": (country_info.get("value") or "").strip(),
                "countryiso3code": r.get("countryiso3code") or "",
                "date": r.get("date"),
                "value": _coerce_value(r),
                "unit": r.get("unit") or "",
                "title": indicator_info.get("value") or indicator,
            }
        )

    if not rows:
        raise EmptyDataError(
            "worldbank",
            query_params={"indicator": indicator, "country": country, "date": date},
        )

    df = pd.DataFrame(rows)
    return df[_FETCH_COLUMNS]


CONNECTORS = Connectors([worldbank_search, worldbank_fetch])


def load() -> Connectors:
    """Return :data:`CONNECTORS`.

    The World Bank API is keyless, so ``load`` takes no arguments — there
    are no secrets to bind.
    """
    return CONNECTORS
