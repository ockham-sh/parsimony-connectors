"""Destatis (German Federal Statistical Office): fetch + catalog enumeration.

GENESIS-Online (the public Destatis statistics database) is reached via the
new ``/genesisGONLINE/api/rest/*`` endpoints. The legacy
``/genesisWS/rest/2020/*`` API is fully retired upstream and now redirects all
traffic to the announcement page; the rewrite below switches both connectors
to the new API. No GAST/registered credentials are required on the new base —
the SPA's anonymous calls work for both metadata and data.

API base: https://www-genesis.destatis.de/genesisGONLINE/api/rest

Endpoints used:

* ``GET /statistics`` — list of statistics (~331 rows)
* ``GET /statistics/{code}/tables`` — tables under a statistic
* ``GET /tables/{code}/information`` — per-table metadata (DE/EN names,
  variable list)
* ``GET /statistics/{code}/information`` — per-statistic metadata (DE/EN
  names plus the long German "Qualitätsbericht" description)
* ``GET /tables/{code}/data`` — JSON-stat 2.0 data for a table

The new API is undocumented; error handling stays defensive — any non-2xx
status maps to ``ProviderError``, any HTML body raises ``ProviderError`` with
"API may have changed".
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, ParseError, ProviderError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport import map_http_error
from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)


_BASE_URL = "https://www-genesis.destatis.de/genesisGONLINE/api/rest"
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _BROWSER_USER_AGENT,
    "Origin": "https://www-genesis.destatis.de",
    "Referer": "https://www-genesis.destatis.de/datenbank/online/",
    "Accept": "application/json",
}

# Concurrency for the per-statistic fan-out. Destatis is happy at 4 parallel
# clients with a 0.25s inter-request delay; bumping concurrency triggers
# 429s and (rarely) 503s.
_METADATA_CONCURRENCY = 4
_INTER_REQUEST_DELAY_S = 0.25
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_RETRY_BACKOFFS_S: tuple[float, ...] = (1.0, 2.0, 4.0)

# Cap the German "Qualitätsbericht" descriptions before they reach the
# embedder — median length is 10–20k chars, which dwarfs MiniLM's ~512 token
# context. 1500 chars (~250 tokens) preserves the lead paragraph and stays
# well within the model.
_DESCRIPTION_CHAR_CAP = 1500


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


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class DestatisFetchParams(BaseModel):
    """Parameters for fetching Destatis table data.

    The new ``/genesisGONLINE/api/rest/tables/{code}/data`` endpoint takes
    the table code in the URL path. We accept it via the canonical ``name``
    field (matches the legacy GENESIS query param key); ``table_id`` remains
    available as an alias for backwards compatibility with code that still
    constructs ``DestatisFetchParams(table_id=...)``.
    """

    model_config = ConfigDict(populate_by_name=True)

    name: Annotated[str, "ns:destatis"] = Field(
        ...,
        alias="table_id",
        description="GENESIS table code (e.g. 61111-0001).",
    )
    start_year: str | None = Field(default=None, description="Start year (YYYY) — best-effort filter.")
    end_year: str | None = Field(default=None, description="End year (YYYY) — best-effort filter.")

    @field_validator("name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name (table code) must be non-empty")
        return v


class DestatisEnumerateParams(BaseModel):
    """No parameters needed — enumerates all GENESIS-Online statistics."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

DESTATIS_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # KEY: bare statistic or table code. Destatis codes are
        # unambiguous — tables always contain a hyphen (``61111-0001``)
        # while statistic codes never do (``61111``) — so no synthetic
        # prefix is needed.
        Column(name="code", role=ColumnRole.KEY, namespace="destatis"),
        # TITLE: ``name.en`` if present else ``name.de``.
        Column(name="title", role=ColumnRole.TITLE),
        # DESCRIPTION feeds the multilingual embedder via
        # ``semantic_text()``. For statistics this is the rich German
        # "Qualitätsbericht" lead; for tables it lifts the parent
        # statistic's description so per-table queries still hit the
        # narrative signal.
        Column(name="description", role=ColumnRole.DESCRIPTION),
        # METADATA columns:
        Column(name="entity_type", role=ColumnRole.METADATA),  # "statistic" | "table"
        Column(name="parent_statistic", role=ColumnRole.METADATA),
        Column(name="subject_area", role=ColumnRole.METADATA),
        Column(name="title_de", role=ColumnRole.METADATA),
        Column(name="title_en", role=ColumnRole.METADATA),
        Column(name="variable_codes", role=ColumnRole.METADATA),  # CSV string
        Column(name="variable_names_en", role=ColumnRole.METADATA),  # CSV string
        Column(name="source", role=ColumnRole.METADATA),  # constant "genesis_online"
    ]
)

DESTATIS_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, param_key="name", namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

_ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "entity_type",
    "parent_statistic",
    "subject_area",
    "title_de",
    "title_en",
    "variable_codes",
    "variable_names_en",
    "source",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_german_number(s: str) -> str:
    """Convert German numeric format (``1.234,56``) to canonical ``1234.56``."""
    s = s.strip()
    s = s.replace(".", "").replace(",", ".")
    return s


def _normalize_german_date(s: str) -> str:
    """Best-effort coercion of a German period label to ISO ``YYYY-MM-DD``.

    Handles the four common formats Destatis emits in dimension labels:
    plain year, ``N. Quartal YYYY``, German month names, and ISO-ish
    ``YYYY-MM`` truncations. Unknown shapes pass through unchanged so we
    never silently corrupt an unfamiliar period.
    """
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


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header; ``None`` if absent/malformed."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _looks_like_html(text: str) -> bool:
    """Heuristic: did the new API silently swap us onto the SPA shell?"""
    head = text[:512].lower()
    return "<html" in head or "<!doctype html" in head


async def _get_json(
    client: httpx.AsyncClient,
    path: str,
    *,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any] | None:
    """GET ``{_BASE_URL}{path}`` and return the parsed JSON body.

    Retries 429/5xx with exponential backoff and honors ``Retry-After``.
    On exhausted retries / network errors / non-JSON responses, logs a
    WARNING and returns ``None``. Callers decide whether the row is
    skippable.
    """
    url = f"{_BASE_URL}{path}"
    async with semaphore:
        await asyncio.sleep(_INTER_REQUEST_DELAY_S)
        last_status: int | None = None
        last_error: str | None = None
        for attempt, backoff in enumerate((*_RETRY_BACKOFFS_S, None)):
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if backoff is None:
                    break
                await asyncio.sleep(backoff)
                continue

            if response.status_code == 200:
                if _looks_like_html(response.text):
                    logger.warning(
                        "Destatis %s returned HTML body; API may have changed",
                        path,
                    )
                    return None
                try:
                    return response.json()
                except ValueError as exc:
                    logger.warning("Destatis %s returned non-JSON body: %s", path, exc)
                    return None

            last_status = response.status_code
            if response.status_code in _RETRY_STATUSES and backoff is not None:
                wait = _retry_after_seconds(response) or backoff
                logger.info(
                    "Destatis %s returned %s (attempt %d); retrying in %.1fs",
                    path,
                    response.status_code,
                    attempt + 1,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            break

        logger.warning(
            "Destatis fetch failed for %s after retries (last_status=%s, last_error=%s)",
            path,
            last_status,
            last_error,
        )
        return None


def _pick_lang(node: Any, key: str = "name") -> tuple[str, str]:
    """Extract DE/EN strings from a Destatis ``{de, en}``-shaped node.

    The new API uses several flavors:

    * ``{"name": {"de": "...", "en": "..."}}``
    * ``{"name_de": "...", "name_en": "..."}``
    * a bare string

    We tolerate all three and return ``(de, en)``; either may be empty.
    """
    if not isinstance(node, dict):
        bare = (str(node).strip() if node is not None else "")
        return bare, bare

    # Shape 1: ``{key: {"de": ..., "en": ...}}``
    nested = node.get(key)
    if isinstance(nested, dict):
        return (str(nested.get("de", "") or "").strip(), str(nested.get("en", "") or "").strip())

    # Shape 2: ``{key_de: ..., key_en: ...}``
    de = str(node.get(f"{key}_de", "") or "").strip()
    en = str(node.get(f"{key}_en", "") or "").strip()
    if de or en:
        return de, en

    # Shape 3: ``{key: "..."}`` — same string for both languages.
    bare = str(node.get(key, "") or "").strip()
    return bare, bare


def _statistic_description(
    *,
    subject_area: str,
    name_de: str,
    name_en: str,
    description_de: str,
    n_tables: int,
) -> str:
    """Assemble the per-statistic DESCRIPTION fed to the embedder.

    Concatenates subject area + DE/EN names + the truncated German
    Qualitätsbericht. Caps the description at ``_DESCRIPTION_CHAR_CAP``
    chars so the embedder never sees a 20k-char wall of text.
    """
    parts: list[str] = []
    if subject_area:
        parts.append(subject_area)
    if name_en and name_en != name_de:
        parts.append(f"{name_de} ({name_en})" if name_de else name_en)
    elif name_de:
        parts.append(name_de)
    if description_de:
        parts.append(description_de[:_DESCRIPTION_CHAR_CAP])
    parts.append(f"German Federal Statistical Office (Destatis), {n_tables} tables.")
    return ". ".join(p for p in parts if p)


def _table_description(
    *,
    table_title: str,
    parent_code: str,
    parent_title_de: str,
    parent_title_en: str,
    parent_description_de: str,
    variable_names_en: list[str],
) -> str:
    """Assemble the per-table DESCRIPTION fed to the embedder.

    Lifts the parent statistic's German description so per-table semantic
    queries still hit the long-form narrative — without this, table rows
    would only carry their short title and miss most retrieval signal.
    """
    parent_title = parent_title_en or parent_title_de
    parts: list[str] = []
    if table_title:
        parts.append(table_title)
    if parent_title:
        parts.append(f"Parent statistic: {parent_title} ({parent_code})")
    if parent_description_de:
        parts.append(parent_description_de[:_DESCRIPTION_CHAR_CAP])
    if variable_names_en:
        parts.append(f"Variables: {', '.join(variable_names_en[:6])}")
    parts.append("Source: Destatis (Statistisches Bundesamt), GENESIS-Online.")
    return ". ".join(p for p in parts if p)


def _extract_variables(info_payload: dict[str, Any] | None) -> tuple[list[str], list[str]]:
    """Extract ``(variable_codes, variable_names_en)`` from a table-info payload.

    The new API's ``/tables/{code}/information`` response carries a
    ``variables`` array; each entry exposes a ``code`` and a localized
    ``name`` (or ``name_en``). Defensive against missing keys / unexpected
    shapes — returns empty lists if anything is off.
    """
    if not isinstance(info_payload, dict):
        return [], []
    variables = info_payload.get("variables") or info_payload.get("Variables") or []
    if not isinstance(variables, list):
        return [], []
    codes: list[str] = []
    names_en: list[str] = []
    for var in variables:
        if not isinstance(var, dict):
            continue
        code = str(var.get("code") or var.get("Code") or "").strip()
        if code:
            codes.append(code)
        _, en = _pick_lang(var, "name")
        if en:
            names_en.append(en)
    return codes, names_en


# ---------------------------------------------------------------------------
# JSON-stat 2.0 parsing (for destatis_fetch)
# ---------------------------------------------------------------------------


def _parse_jsonstat(payload: dict[str, Any], table_code: str) -> pd.DataFrame:
    """Parse a JSON-stat 2.0 dataset into a long-format DataFrame.

    The minimal shape we rely on:

    .. code-block:: json

       {
         "label": "...",
         "value": [<float | null>, ...] | {<idx>: <float>},
         "id": ["TimeDim", "OtherDim"],
         "size": [12, 3],
         "dimension": {
           "TimeDim": {"category": {"index": [...], "label": {...}}},
           "OtherDim": {"category": {...}}
         }
       }

    Output columns: ``series_id``, ``date``, ``value``, plus one column per
    non-time dimension (so a 2-D table flattens to ``date × dim → value``).
    A ``unit`` column is added when JSON-stat metadata carries one.
    """
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

    # JSON-stat permits either a list (dense) or a dict (sparse) for
    # ``value``. Normalize to a flat list of length ``total``.
    if isinstance(raw_values, list):
        values_flat: list[Any] = list(raw_values)
        # Pad/truncate defensively in case sizes drift from the array length.
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

    # Build per-dimension category index→label maps.
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
            # ``{<key>: <position>}`` — invert to position-ordered list.
            ordered_pairs = sorted(((int(v), str(k)) for k, v in index.items()), key=lambda p: p[0])
            ordered = [k for _, k in ordered_pairs]
        else:
            ordered = []
        labels = category.get("label") or {}
        if not isinstance(labels, dict):
            labels = {}
        dim_indices.append(ordered)
        dim_labels.append({str(k): str(v) for k, v in labels.items()})

    # Identify the time dimension (best effort — Destatis labels it Zeit /
    # ZEIT / Time / a code containing "ZEIT"). Fallback: dimension 0.
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

    # Walk the flat ``value`` array decoding each cell's coordinate.
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

    # Expose unit metadata if JSON-stat carries it.
    role = payload.get("role") or {}
    if isinstance(role, dict):
        unit_dims = role.get("unit") or []
        if isinstance(unit_dims, list) and unit_dims:
            df["unit"] = ",".join(str(u) for u in unit_dims)

    return df


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(
    env={"username": "DESTATIS_USERNAME", "password": "DESTATIS_PASSWORD"},
    output=DESTATIS_FETCH_OUTPUT,
    tags=["macro", "de"],
)
async def destatis_fetch(
    params: DestatisFetchParams,
    *,
    username: str = "GAST",
    password: str = "GAST",
) -> Result:
    """Fetch a Destatis GENESIS table by table code.

    Hits the public ``/genesisGONLINE/api/rest/tables/{code}/data`` endpoint
    and parses the JSON-stat 2.0 response into a long-format DataFrame.

    The new API does not require credentials — ``username`` and ``password``
    are accepted for backward compatibility but ignored. ``start_year`` /
    ``end_year`` are passed as query params on a best-effort basis; if the
    new API does not support them the full series is fetched and the caller
    should filter downstream.
    """
    # ``username``/``password`` retained for backward compat; the new
    # GENESIS-Online API is fully anonymous and ignores them. Reference
    # them so linters don't flag unused params.
    del username, password

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
        headers=_HEADERS,
    ) as client:
        response = await client.get(f"{_BASE_URL}{path}", params=query or None)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="destatis", op_name="data")

    text = response.text
    # Safety net: the legacy API used to redirect GAST traffic to an HTML
    # announcement page. The new API shouldn't, but if Destatis ever
    # re-routes us we want a clear error rather than a confusing parse
    # failure downstream.
    if (
        _looks_like_html(text)
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

    # The new GENESIS-Online API wraps datasets in ``{"data": [<dataset>, ...]}``
    # rather than emitting bare JSON-stat at the top level. Each dataset is
    # JSON-stat-shaped (id/size/dimension/value). We concatenate all
    # datasets in the response so multi-dataset tables are exposed as a
    # single long DataFrame.
    datasets: list[dict[str, Any]]
    raw_data = payload.get("data")
    if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict):
        datasets = [d for d in raw_data if isinstance(d, dict)]
    elif isinstance(raw_data, dict):
        datasets = [raw_data]
    elif "id" in payload and "value" in payload:
        # Bare JSON-stat envelope (older shape) — parse directly.
        datasets = [payload]
    else:
        raise ParseError(
            provider="destatis",
            message=f"Unexpected JSON-stat envelope for {table_code}: keys={list(payload.keys())}",
        )

    frames = [_parse_jsonstat(d, table_code) for d in datasets]
    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    return Result.from_dataframe(
        df,
        Provenance(
            source="destatis",
            params={"name": table_code},
            properties={"source_url": "https://www-genesis.destatis.de/datenbank/online/"},
        ),
    )


@enumerator(
    env={"username": "DESTATIS_USERNAME", "password": "DESTATIS_PASSWORD"},
    output=DESTATIS_ENUMERATE_OUTPUT,
    tags=["macro", "de"],
)
async def enumerate_destatis(
    params: DestatisEnumerateParams,
    *,
    username: str = "GAST",
    password: str = "GAST",
) -> pd.DataFrame:
    """Enumerate every Destatis statistic + table on GENESIS-Online.

    Pipeline:

    1. ``GET /statistics`` — single call, returns ~331 statistics.
    2. For each statistic, in parallel (concurrency=4, 0.25s inter-request
       delay), call ``GET /statistics/{code}/information`` to pick up the
       long German "Qualitätsbericht" description and
       ``GET /statistics/{code}/tables`` to enumerate its tables.
    3. Emit one ``entity_type='statistic'`` row keyed by the bare statistic
       code (Destatis codes are unambiguous: tables always contain a
       hyphen, statistic codes never do).
    4. Emit one ``entity_type='table'`` row per table (~2,999 in total),
       keyed by the table code (e.g. ``61111-0001``); the parent
       statistic's German description is lifted into the table's
       ``description`` so per-table semantic queries still see the
       narrative signal.

    On 429/5xx we retry 3× with exponential backoff and honor
    ``Retry-After``. After exhausting retries the per-statistic call logs a
    WARNING and that statistic is skipped — the catalog stays useful even
    if a few entries fail.
    """
    del params, username, password  # all unused; kept for backward compat

    semaphore = asyncio.Semaphore(_METADATA_CONCURRENCY)
    rows: list[dict[str, str]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=_HEADERS) as client:
        index = await _get_json(client, "/statistics", semaphore=semaphore)
        if index is None:
            logger.warning("Destatis enumerate: /statistics fetch failed; emitting empty catalog")
            return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))

        statistics = _extract_statistics_list(index)
        if not statistics:
            logger.warning("Destatis enumerate: /statistics returned 0 entries")
            return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))

        # Per-statistic fan-out: we need both the rich DE description from
        # ``/information`` and the table list from ``/tables``. Run them
        # concurrently per statistic so we double up the throughput
        # without blowing the per-call delay budget.
        async def _gather_one(
            stat: dict[str, Any],
        ) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
            code = str(stat.get("code") or stat.get("Code") or "").strip()
            if not code:
                return stat, None, None
            info_task = _get_json(client, f"/statistics/{code}/information", semaphore=semaphore)
            tables_task = _get_json(client, f"/statistics/{code}/tables", semaphore=semaphore)
            info, tables = await asyncio.gather(info_task, tables_task)
            return stat, info, tables

        results = await asyncio.gather(*[_gather_one(s) for s in statistics])

    failed: list[str] = []
    for stat, info, tables_payload in results:
        stat_code = str(stat.get("code") or stat.get("Code") or "").strip()
        if not stat_code:
            continue
        if info is None and tables_payload is None:
            failed.append(stat_code)
            continue

        rows.extend(
            _emit_rows_for_statistic(
                stat=stat,
                info=info or {},
                tables_payload=tables_payload or {},
            )
        )

    if failed:
        logger.info(
            "Destatis enumerate: %d/%d statistics failed metadata fetch: %s",
            len(failed),
            len(statistics),
            ", ".join(failed[:20]),
        )
    else:
        logger.info("Destatis enumerate: %d statistics fetched successfully", len(statistics))

    columns = list(_ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


def _extract_statistics_list(index_payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """Normalize the various wrapping shapes into a list of statistic dicts.

    Tolerates:

    * ``[ {...}, {...} ]`` — bare list at the top level
    * ``{"statistics": [...]}`` / ``{"Statistics": [...]}``
    * ``{"items": [...]}``
    """
    if isinstance(index_payload, list):
        return [s for s in index_payload if isinstance(s, dict)]
    if not isinstance(index_payload, dict):
        return []
    for key in ("statistics", "Statistics", "items", "Items"):
        candidate = index_payload.get(key)
        if isinstance(candidate, list):
            return [s for s in candidate if isinstance(s, dict)]
    return []


def _extract_tables_list(tables_payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """Same shape-tolerance as ``_extract_statistics_list`` but for tables."""
    if isinstance(tables_payload, list):
        return [t for t in tables_payload if isinstance(t, dict)]
    if not isinstance(tables_payload, dict):
        return []
    for key in ("tables", "Tables", "items", "Items"):
        candidate = tables_payload.get(key)
        if isinstance(candidate, list):
            return [t for t in candidate if isinstance(t, dict)]
    return []


def _emit_rows_for_statistic(
    *,
    stat: dict[str, Any],
    info: dict[str, Any],
    tables_payload: dict[str, Any],
) -> list[dict[str, str]]:
    """Produce one statistic row + one row per child table."""
    stat_code = str(stat.get("code") or stat.get("Code") or "").strip()
    name_de, name_en = _pick_lang(stat, "name")
    if not name_de and not name_en:
        # Fall back to ``information`` payload if the index entry lacks names.
        name_de, name_en = _pick_lang(info, "name")

    subject_area = str(
        stat.get("subjectArea")
        or stat.get("subject_area")
        or stat.get("SubjectArea")
        or info.get("subjectArea")
        or ""
    ).strip()

    description_de = str(
        info.get("description", {}).get("de")
        if isinstance(info.get("description"), dict)
        else (info.get("description_de") or info.get("description") or "")
    ).strip()

    tables = _extract_tables_list(tables_payload)
    n_tables = len(tables)

    statistic_title = name_en or name_de or stat_code
    statistic_description = _statistic_description(
        subject_area=subject_area,
        name_de=name_de,
        name_en=name_en,
        description_de=description_de,
        n_tables=n_tables,
    )

    rows: list[dict[str, str]] = [
        {
            "code": stat_code,
            "title": statistic_title,
            "description": statistic_description,
            "entity_type": "statistic",
            "parent_statistic": "",
            "subject_area": subject_area,
            "title_de": name_de,
            "title_en": name_en,
            "variable_codes": "",
            "variable_names_en": "",
            "source": "genesis_online",
        }
    ]

    for table in tables:
        table_code = str(table.get("code") or table.get("Code") or "").strip()
        if not table_code:
            continue
        table_de, table_en = _pick_lang(table, "name")
        table_title = table_en or table_de or table_code

        # ``/statistics/{code}/tables`` typically inlines each table's
        # variables. If absent we fall back to empty lists rather than a
        # second per-table HTTP call (we already issue 2 requests per
        # statistic and 3,000 tables × another roundtrip is too slow).
        var_codes, var_names_en = _extract_variables(table)

        rows.append(
            {
                "code": table_code,
                "title": table_title,
                "description": _table_description(
                    table_title=table_title,
                    parent_code=stat_code,
                    parent_title_de=name_de,
                    parent_title_en=name_en,
                    parent_description_de=description_de,
                    variable_names_en=var_names_en,
                ),
                "entity_type": "table",
                "parent_statistic": stat_code,
                "subject_area": subject_area,
                "title_de": table_de,
                "title_en": table_en,
                "variable_codes": ",".join(var_codes),
                "variable_names_en": ",".join(var_names_en),
                "source": "genesis_online",
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_destatis.search import (  # noqa: E402  (after public decorators)
    DESTATIS_SEARCH_OUTPUT,
    PARSIMONY_DESTATIS_CATALOG_URL_ENV,
    DestatisSearchParams,
    destatis_search,
)

CATALOGS: list[tuple[str, object]] = [("destatis", enumerate_destatis)]

CONNECTORS = Connectors([destatis_fetch, enumerate_destatis, destatis_search])

__all__ = [
    "CATALOGS",
    "CONNECTORS",
    "DESTATIS_ENUMERATE_OUTPUT",
    "DESTATIS_FETCH_OUTPUT",
    "DESTATIS_SEARCH_OUTPUT",
    "DestatisEnumerateParams",
    "DestatisFetchParams",
    "DestatisSearchParams",
    "PARSIMONY_DESTATIS_CATALOG_URL_ENV",
    "destatis_fetch",
    "destatis_search",
    "enumerate_destatis",
]
