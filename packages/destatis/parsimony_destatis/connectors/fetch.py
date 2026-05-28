"""Destatis table fetch connector."""

from __future__ import annotations

import re
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, ParseError, ProviderError
from parsimony.transport import map_http_error

from parsimony_destatis._http import BASE_URL, HEADERS, looks_like_html
from parsimony_destatis.outputs import DESTATIS_FETCH_OUTPUT
from parsimony_destatis.params import DestatisFetchParams

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


def _parse_jsonstat(payload: dict[str, Any], table_code: str) -> pd.DataFrame:
    """Parse a JSON-stat 2.0 dataset into a long-format DataFrame."""
    label = str(payload.get("label") or table_code)

    dim_ids = payload.get("id") or []
    sizes = payload.get("size") or []
    dimensions = payload.get("dimension") or {}
    raw_values = payload.get("value")

    if not isinstance(dim_ids, list) or not isinstance(sizes, list):
        raise ParseError(
            provider="destatis",
            message=f"JSON-stat payload for {table_code} missing id/size arrays",
        )
    if len(dim_ids) != len(sizes):
        raise ParseError(
            provider="destatis",
            message=f"JSON-stat id/size length mismatch for {table_code}",
        )

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

    def _is_time(idx: int, dim_id: str) -> bool:
        upper = dim_id.upper()
        return "ZEIT" in upper or upper in {"TIME", "JAHR", "MONAT", "QUARTAL"}

    time_dim_idx: int | None = None
    for i, did in enumerate(dim_ids):
        if _is_time(i, str(did)):
            time_dim_idx = i
            break
    if time_dim_idx is None and dim_ids:
        time_dim_idx = 0

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
                row["date"] = _normalize_german_date(cat_label or cat_key)
            else:
                row[dim_id] = cat_label or cat_key

        if "date" not in row:
            row["date"] = ""
        rows.append(row)

    if not rows:
        raise EmptyDataError(
            provider="destatis",
            message=f"No observations parsed from JSON-stat for table {table_code}",
        )

    df = pd.DataFrame(rows)

    role = payload.get("role") or {}
    if isinstance(role, dict):
        unit_dims = role.get("unit") or []
        if isinstance(unit_dims, list) and unit_dims:
            df["unit"] = ",".join(str(u) for u in unit_dims)

    return df


@connector(output=DESTATIS_FETCH_OUTPUT, tags=["macro", "de"])
async def destatis_fetch(
    name: Annotated[str, "ns:destatis"],
    start_year: str | None = None,
    end_year: str | None = None,
) -> pd.DataFrame:
    """Fetch a Destatis GENESIS table by table code.

    Hits the public ``/genesisGONLINE/api/rest/tables/{code}/data`` endpoint
    and parses the JSON-stat 2.0 response into a long-format DataFrame.
    """
    params = DestatisFetchParams(table_id=name, start_year=start_year, end_year=end_year)
    table_code = params.name
    path = f"/tables/{table_code}/data"

    query: dict[str, str] = {}
    if params.start_year:
        query["startyear"] = params.start_year
    if params.end_year:
        query["endyear"] = params.end_year

    async with httpx.AsyncClient(
        timeout=60.0,
        follow_redirects=True,
        headers=HEADERS,
    ) as client:
        response = await client.get(f"{BASE_URL}{path}", params=query or None)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="destatis", op_name="data")

    text = response.text
    if (
        looks_like_html(text)
        or "announcement" in text.lower()
        or "datenbank/online" in str(response.url)
    ):
        raise ProviderError(
            provider="destatis",
            status_code=0,
            message=(
                "Destatis returned an HTML announcement page instead of JSON-stat data. "
                "API may have changed; please file an issue."
            ),
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise ParseError(
            provider="destatis",
            message=f"Failed to parse JSON-stat for {table_code}: {exc}",
        ) from exc

    if not isinstance(payload, dict):
        raise ParseError(
            provider="destatis",
            message=f"JSON-stat response for {table_code} was not an object",
        )

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
            provider="destatis",
            message=f"Unexpected JSON-stat envelope for {table_code}: keys={list(payload.keys())}",
        )

    frames = [_parse_jsonstat(d, table_code) for d in datasets]
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
