"""Banco de Portugal (BdP): fetch + catalog enumeration.

API base: ``https://bpstat.bportugal.pt/data/v1`` (BPstat). No authentication
required. The catalog enumerator is series-grained — every individual time
series across BdP's 65 leaf domains is published as its own row, alongside
synthetic ``domain:`` and ``dataset:`` parent rows so agents can navigate the
hierarchy from search hits.

Endpoints used:

* ``GET /domains/?lang=EN`` — list of 77 domains (we keep the 65 with
  ``has_series=true``).
* ``GET /domains/{domain_id}/datasets/?lang=EN`` — list of datasets in a
  domain (returned via JSON-stat ``link.item[]``).
* ``GET /domains/{domain_id}/datasets/{dataset_id}/?lang=EN[&page=N]`` — per
  dataset detail in JSON-stat 2.0 envelope. The endpoint is paginated at a
  fixed 10 series per page (the ``limit`` query param is ignored upstream).
  ``extension.series[]`` carries the human-readable series ``label``;
  ``extension.next_page`` (full URL) is the pagination cursor.

The ``value`` array (the actual observations) is unavoidable — neither
``limit=0`` nor ``obs_since=2099-01-01`` suppress observation traffic
without dropping the series stubs entirely. We accept the bandwidth cost
(~40 KB / page) and ignore the values after parsing.

WAF posture: Akamai-fronted; conservative throttling (concurrency=4,
0.25 s inter-request delay, browser User-Agent, 1/2/4 s backoff on
429/5xx) keeps enumeration stable.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any
from urllib.parse import parse_qs, urlparse

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport import map_http_error
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


_BASE_URL = "https://bpstat.bportugal.pt/data/v1"

# WAF / throttling. BPstat sits behind Akamai; the values below are
# conservative defaults that have empirically held up across the full
# enumeration (~7,200 dataset-detail pages at ~0.4–0.6 s each).
_METADATA_CONCURRENCY = 4
_INTER_REQUEST_DELAY_S = 0.25
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _BROWSER_USER_AGENT,
    "Accept": "application/json",
    "Origin": "https://bpstat.bportugal.pt",
    "Referer": "https://bpstat.bportugal.pt/",
}
_RETRY_STATUSES = frozenset({403, 429, 500, 502, 503, 504})
_RETRY_BACKOFFS_S: tuple[float, ...] = (1.0, 2.0, 4.0)

# Cap descriptions before they reach the embedder. BdP labels run up to a
# few hundred chars and rarely warrant truncation, but a hard cap keeps the
# embedder context-window-safe (Destatis pattern).
_DESCRIPTION_CHAR_CAP = 1500

# Request budget guard. The BdP catalog has ~72 K series across ~7,200
# pages; nothing should ever exceed this in normal operation, but we cap
# per-dataset pagination defensively so a runaway ``next_page`` cycle
# can't exhaust the process. A single dataset can in principle hold tens
# of thousands of series; 5,000 pages × 10 series/page = 50 K is well
# above the largest observed dataset (16,644 series → 1,665 pages).
_MAX_PAGES_PER_DATASET = 5_000


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BdpFetchParams(BaseModel):
    """Parameters for fetching Banco de Portugal time series."""

    domain_id: int = Field(..., description="Domain ID (use enumerate to discover)")
    dataset_id: Annotated[str, "ns:bdp"] = Field(..., description="Dataset ID within the domain")
    series_ids: str | None = Field(
        default=None,
        description="Comma-separated series IDs to filter (optional)",
    )
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    lang: str = Field(default="en", description="Language: en or pt")

    @field_validator("dataset_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("dataset_id must be non-empty")
        return v


class BdpEnumerateParams(BaseModel):
    """No parameters needed — enumerates BdP domains, datasets and series."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BDP_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # KEY shape:
        # * series rows  — ``"{domain_id}:{dataset_id}:{series_id}"``
        #                  (e.g. ``"1:921a2108…:12395488"``)
        # * dataset rows — ``"dataset:{domain_id}:{dataset_id}"``
        # * domain rows  — ``"domain:{domain_id}"``
        # The synthetic prefixes mirror BoJ's ``db:`` and Destatis'
        # statistic/table convention so downstream consumers can split
        # entity types by KEY alone.
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.DESCRIPTION),
        Column(name="entity_type", role=ColumnRole.METADATA),  # "domain" | "dataset" | "series"
        Column(name="domain_id", role=ColumnRole.METADATA),
        Column(name="domain_name", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(name="dataset_label", role=ColumnRole.METADATA),
        Column(name="title_pt", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="units", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
        Column(name="last_update", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),  # constant "bpstat"
    ]
)

BDP_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, param_key="dataset_id", namespace="bdp"),
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
    "domain_id",
    "domain_name",
    "dataset_id",
    "dataset_label",
    "title_pt",
    "frequency",
    "units",
    "start_date",
    "end_date",
    "last_update",
    "source",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header; ``None`` if absent/malformed."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    semaphore: asyncio.Semaphore,
    params: dict[str, str] | None = None,
) -> Any | None:
    """GET ``url`` and return the parsed JSON body.

    Retries 403/429/5xx with exponential backoff and honors ``Retry-After``.
    On exhausted retries / network errors / non-JSON bodies, logs a WARNING
    and returns ``None`` so the caller can decide whether to skip.
    """
    async with semaphore:
        await asyncio.sleep(_INTER_REQUEST_DELAY_S)
        last_status: int | None = None
        last_error: str | None = None
        for attempt, backoff in enumerate((*_RETRY_BACKOFFS_S, None)):
            try:
                response = await client.get(url, params=params)
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if backoff is None:
                    break
                await asyncio.sleep(backoff)
                continue

            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as exc:
                    logger.warning("BdP %s returned non-JSON body: %s", url, exc)
                    return None

            last_status = response.status_code
            if response.status_code in _RETRY_STATUSES and backoff is not None:
                wait = _retry_after_seconds(response) or backoff
                logger.info(
                    "BdP %s returned %s (attempt %d); retrying in %.1fs",
                    url,
                    response.status_code,
                    attempt + 1,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            break

        logger.warning(
            "BdP fetch failed for %s after retries (last_status=%s, last_error=%s)",
            url,
            last_status,
            last_error,
        )
        return None


def _truncate(text: str, cap: int = _DESCRIPTION_CHAR_CAP) -> str:
    """Cap a string at ``cap`` chars; return as-is if shorter."""
    if not text:
        return ""
    if len(text) <= cap:
        return text
    return text[:cap].rstrip()


def _series_description(
    *,
    title: str,
    title_pt: str,
    dataset_label: str,
    domain_name: str,
    frequency: str,
    units: str,
    domain_id: str,
    dataset_id: str,
) -> str:
    """Assemble the per-series DESCRIPTION fed to the embedder.

    Folds the Portuguese title into the tail as a keyword fragment so
    Portuguese queries still hit the row via subword overlap, even when
    the catalog is embedded with an English-only model.
    """
    chunks: list[str] = []
    if title:
        chunks.append(f"{title}.")
    if dataset_label:
        chunks.append(f"{dataset_label}.")
    if domain_name:
        chunks.append(f"Domain: {domain_name}.")
    if frequency:
        chunks.append(f"Frequency: {frequency}.")
    if units:
        chunks.append(f"Unit: {units}.")
    chunks.append(f"Banco de Portugal BPstat (domain={domain_id}, dataset={dataset_id}).")
    if title_pt and title_pt.strip() and title_pt.strip().lower() != (title or "").strip().lower():
        chunks.append(f"PT: {title_pt}.")
    return _truncate(" ".join(c for c in chunks if c).strip())


def _dataset_description(
    *,
    dataset_label: str,
    domain_name: str,
    domain_id: str,
    dataset_id: str,
    num_series: int,
) -> str:
    """Assemble the per-dataset DESCRIPTION fed to the embedder."""
    parts = [
        f"{dataset_label}." if dataset_label else "",
        f"Banco de Portugal dataset under domain '{domain_name}'.",
        f"Holds {num_series} series." if num_series else "",
        f"Fetch via bdp_fetch(domain_id={domain_id}, dataset_id='{dataset_id}', series_ids=...).",
    ]
    return _truncate(" ".join(p for p in parts if p).strip())


def _domain_description(*, name: str, description: str, num_series: int, num_datasets: int) -> str:
    """Assemble the per-domain DESCRIPTION fed to the embedder."""
    boilerplate = "Banco de Portugal economic statistics."
    body = f"{boilerplate} {description}" if description else boilerplate
    parts = [
        f"BPstat domain: {name}." if name else "",
        body,
        f"Holds {num_datasets} datasets and {num_series} series." if (num_datasets or num_series) else "",
    ]
    return _truncate(" ".join(p for p in parts if p).strip())


def _frequency_from_dimension(dataset_payload: dict[str, Any]) -> str:
    """Pull the dataset-level frequency from JSON-stat dimensions.

    BdP datasets carry a ``Periodicity`` dimension whose category labels
    spell out the frequency in plain English (``Monthly``, ``Quarterly``,
    ``Yearly``…). We pick the first category label as a representative
    frequency for the dataset; if the dimension is absent we return ``""``.
    """
    dims = dataset_payload.get("dimension")
    if not isinstance(dims, dict):
        return ""
    for _key, dim in dims.items():
        if not isinstance(dim, dict):
            continue
        label = str(dim.get("label") or "").lower()
        if "periodicity" in label or "frequency" in label:
            cat = dim.get("category")
            if not isinstance(cat, dict):
                continue
            labels = cat.get("label")
            if isinstance(labels, dict) and labels:
                return str(next(iter(labels.values())))
    return ""


def _units_from_dimension(dataset_payload: dict[str, Any]) -> str:
    """Pull the dataset-level unit-of-measure from JSON-stat dimensions."""
    dims = dataset_payload.get("dimension")
    if not isinstance(dims, dict):
        return ""
    for _key, dim in dims.items():
        if not isinstance(dim, dict):
            continue
        label = str(dim.get("label") or "").lower()
        if "unit" in label:
            cat = dim.get("category")
            if not isinstance(cat, dict):
                continue
            labels = cat.get("label")
            if isinstance(labels, dict) and labels:
                return str(next(iter(labels.values())))
    return ""


def _time_bounds(dataset_payload: dict[str, Any]) -> tuple[str, str]:
    """Return ``(start_date, end_date)`` from the JSON-stat time dimension."""
    role = dataset_payload.get("role") or {}
    if not isinstance(role, dict):
        return "", ""
    time_dims = role.get("time") or []
    if not isinstance(time_dims, list) or not time_dims:
        return "", ""
    time_dim_key = time_dims[0]
    dim = (dataset_payload.get("dimension") or {}).get(time_dim_key)
    if not isinstance(dim, dict):
        return "", ""
    cat = dim.get("category")
    if not isinstance(cat, dict):
        return "", ""
    index = cat.get("index")
    keys: list[str] = []
    if isinstance(index, list):
        keys = [str(k) for k in index]
    elif isinstance(index, dict):
        # ``{<key>: <position>}`` — invert to position-ordered list.
        keys = [k for _pos, k in sorted(((int(v), str(k)) for k, v in index.items()), key=lambda p: p[0])]
    if not keys:
        return "", ""
    return keys[0], keys[-1]


def _next_page_url(payload: dict[str, Any], current_url: str) -> str | None:
    """Extract the ``next_page`` URL from a dataset-detail payload.

    Returns ``None`` when there is no next page or the payload has drifted
    from the documented shape. Defends against a self-referential cycle by
    refusing to advance to ``current_url`` itself.
    """
    ext = payload.get("extension")
    if not isinstance(ext, dict):
        return None
    next_url = ext.get("next_page")
    if not isinstance(next_url, str) or not next_url:
        return None
    if _normalize_page_url(next_url) == _normalize_page_url(current_url):
        return None
    return next_url


def _normalize_page_url(url: str) -> str:
    """Strip the host so we can compare URLs across absolute/relative forms."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    return parsed.path + "?" + "&".join(sorted(f"{k}={','.join(v)}" for k, v in qs.items()))


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BDP_FETCH_OUTPUT, tags=["macro", "pt"])
async def bdp_fetch(params: BdpFetchParams) -> Result:
    """Fetch Banco de Portugal time series by domain and dataset ID.

    Uses the BPstat API. Two-step workflow: domain → dataset → observations.
    """
    url = f"{_BASE_URL}/domains/{params.domain_id}/datasets/{params.dataset_id}/"
    req_params: dict[str, str] = {"lang": params.lang.upper()}

    if params.series_ids:
        req_params["series_ids"] = params.series_ids
    if params.start_date:
        req_params["obs_since"] = params.start_date
    if params.end_date:
        req_params["obs_to"] = params.end_date

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(url, params=req_params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="bdp", op_name="observations")
        json_data = response.json()

    # Parse JSON-stat style response
    time_dim_key = None
    role = json_data.get("role", {})
    time_dims = role.get("time", [])
    if time_dims:
        time_dim_key = time_dims[0]

    dimension = json_data.get("dimension", {})
    dates: list[str] = []
    if time_dim_key and time_dim_key in dimension:
        cat = dimension[time_dim_key].get("category", {})
        index = cat.get("index", {})
        if isinstance(index, dict):
            dates = list(index.keys())
        elif isinstance(index, list):
            dates = index

    raw_values = json_data.get("value", [])
    # JSON-stat value can be a dict with string keys or a list
    if isinstance(raw_values, dict):
        values_list: list[Any] = (
            [raw_values.get(str(i)) for i in range(max(int(k) for k in raw_values) + 1)] if raw_values else []
        )
    else:
        values_list = list(raw_values)

    if not dates or not values_list:
        raise EmptyDataError(
            provider="bdp",
            message=f"No observations for domain={params.domain_id}, dataset={params.dataset_id}",
        )

    # Extract series metadata
    series_info = json_data.get("extension", {}).get("series", [])
    n_dates = len(dates)
    n_series = len(values_list) // n_dates if n_dates else 1

    rows: list[dict[str, Any]] = []
    for s_idx in range(n_series):
        if s_idx >= len(series_info):
            # API drift signal: the value array implies more series than
            # the metadata block declared. We fall back to a positional
            # synthetic id, but log so future schema changes surface.
            logger.warning(
                "BdP series index %d exceeds extension.series length %d for "
                "domain=%s dataset=%s; falling back to positional id",
                s_idx,
                len(series_info),
                params.domain_id,
                params.dataset_id,
            )
            sid = str(s_idx)
            label = sid
        else:
            sid = str(series_info[s_idx]["id"])
            label = series_info[s_idx].get("label", sid)

        for d_idx, date_str in enumerate(dates):
            val_idx = s_idx * n_dates + d_idx
            if val_idx >= len(values_list):
                break
            raw = values_list[val_idx]
            try:
                value = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                value = None
            rows.append(
                {
                    "series_id": sid,
                    "title": label,
                    "date": date_str,
                    "value": value,
                }
            )

    if not rows:
        raise EmptyDataError(
            provider="bdp",
            message=f"No observations parsed for domain={params.domain_id}, dataset={params.dataset_id}",
        )

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="bdp",
            params={
                "domain_id": params.domain_id,
                "dataset_id": params.dataset_id,
            },
            properties={"source_url": "https://bpstat.bportugal.pt"},
        ),
    )


# ---------------------------------------------------------------------------
# Enumerator helpers (per-dataset crawl)
# ---------------------------------------------------------------------------


async def _list_domains(client: httpx.AsyncClient, semaphore: asyncio.Semaphore) -> list[dict[str, Any]]:
    """Return the BdP domain list (77 entries).

    Empty list on failure; the caller logs and emits an empty catalog.
    """
    payload = await _get_json(
        client,
        f"{_BASE_URL}/domains/",
        params={"lang": "EN"},
        semaphore=semaphore,
    )
    if not isinstance(payload, list):
        return []
    return [d for d in payload if isinstance(d, dict)]


async def _list_datasets(
    client: httpx.AsyncClient,
    domain_id: int | str,
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Return the dataset stubs under ``domain_id``.

    Each stub carries ``label`` and ``extension.{id, num_series, obs_updated_at}``.
    """
    payload = await _get_json(
        client,
        f"{_BASE_URL}/domains/{domain_id}/datasets/",
        params={"lang": "EN"},
        semaphore=semaphore,
    )
    if not isinstance(payload, dict):
        return []
    items = payload.get("link", {}).get("item", []) if isinstance(payload.get("link"), dict) else []
    if not isinstance(items, list):
        return []
    return [it for it in items if isinstance(it, dict)]


async def _crawl_dataset_series(
    client: httpx.AsyncClient,
    domain_id: int | str,
    dataset_id: str,
    semaphore: asyncio.Semaphore,
    *,
    lang: str = "EN",
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Walk the paginated dataset detail and collect every series stub.

    Returns ``(series_stubs, first_page_payload)``; the first page is
    returned alongside so callers can read dataset-level metadata
    (frequency, units, time bounds) without a second request. ``None`` if
    the very first page failed.
    """
    base = f"{_BASE_URL}/domains/{domain_id}/datasets/{dataset_id}/"
    first_url = base
    first_params = {"lang": lang}
    first_payload = await _get_json(client, first_url, params=first_params, semaphore=semaphore)
    if not isinstance(first_payload, dict):
        return [], None

    series: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def _accumulate(payload: dict[str, Any]) -> None:
        ext = payload.get("extension")
        if not isinstance(ext, dict):
            return
        items = ext.get("series") or []
        if not isinstance(items, list):
            return
        for s in items:
            if not isinstance(s, dict):
                continue
            sid = str(s.get("id") or "").strip()
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            series.append(s)

    _accumulate(first_payload)

    next_url = _next_page_url(first_payload, first_url + "?" + "&".join(f"{k}={v}" for k, v in first_params.items()))
    pages = 1
    while next_url and pages < _MAX_PAGES_PER_DATASET:
        page_payload = await _get_json(client, next_url, semaphore=semaphore)
        if not isinstance(page_payload, dict):
            break
        _accumulate(page_payload)
        next_url = _next_page_url(page_payload, next_url)
        pages += 1

    if pages >= _MAX_PAGES_PER_DATASET:
        logger.warning(
            "BdP dataset %s/%s exceeded page cap (%d); truncating",
            domain_id,
            dataset_id,
            _MAX_PAGES_PER_DATASET,
        )

    return series, first_payload


async def _fetch_pt_labels(
    client: httpx.AsyncClient,
    series_ids: list[str],
    semaphore: asyncio.Semaphore,
) -> dict[str, str]:
    """Bulk-fetch Portuguese labels for ``series_ids`` via ``/series/``.

    The endpoint accepts up to 100 IDs per call and returns a list of
    ``{id, label, ...}`` records. We split into 100-id batches and dedupe
    upstream — invalid IDs are silently dropped by BPstat. Failures (non-
    200, non-JSON) yield no PT label for the affected batch; the catalog
    row falls back to the EN label only.
    """
    out: dict[str, str] = {}
    if not series_ids:
        return out
    BATCH = 100
    batches = [series_ids[i : i + BATCH] for i in range(0, len(series_ids), BATCH)]

    async def _one(batch: list[str]) -> None:
        url = f"{_BASE_URL}/series/"
        params = {"series_ids": ",".join(batch), "lang": "PT"}
        payload = await _get_json(client, url, params=params, semaphore=semaphore)
        if not isinstance(payload, list):
            return
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            sid = str(entry.get("id") or "").strip()
            label = str(entry.get("label") or "").strip()
            if sid and label:
                out[sid] = label

    await asyncio.gather(*[_one(b) for b in batches])
    return out


def _emit_rows_for_dataset(
    *,
    domain_id: str,
    domain_name: str,
    dataset_id: str,
    dataset_label: str,
    dataset_payload: dict[str, Any],
    series_stubs: list[dict[str, Any]],
    pt_labels: dict[str, str],
    last_update: str,
) -> list[dict[str, str]]:
    """Convert a crawled dataset into a list of catalog rows.

    Always emits a ``dataset`` row first, then one ``series`` row per
    discovered stub. Returns an empty list if the payload is malformed.
    """
    frequency = _frequency_from_dimension(dataset_payload)
    units = _units_from_dimension(dataset_payload)
    start_date, end_date = _time_bounds(dataset_payload)

    rows: list[dict[str, str]] = []

    rows.append(
        {
            "code": f"dataset:{domain_id}:{dataset_id}",
            "title": dataset_label,
            "description": _dataset_description(
                dataset_label=dataset_label,
                domain_name=domain_name,
                domain_id=domain_id,
                dataset_id=dataset_id,
                num_series=len(series_stubs),
            ),
            "entity_type": "dataset",
            "domain_id": domain_id,
            "domain_name": domain_name,
            "dataset_id": dataset_id,
            "dataset_label": dataset_label,
            "title_pt": "",
            "frequency": frequency,
            "units": units,
            "start_date": start_date,
            "end_date": end_date,
            "last_update": last_update,
            "source": "bpstat",
        }
    )

    for stub in series_stubs:
        sid = str(stub.get("id") or "").strip()
        if not sid:
            continue
        label = str(stub.get("label") or sid)
        title_pt = pt_labels.get(sid, "")
        rows.append(
            {
                "code": f"{domain_id}:{dataset_id}:{sid}",
                "title": label,
                "description": _series_description(
                    title=label,
                    title_pt=title_pt,
                    dataset_label=dataset_label,
                    domain_name=domain_name,
                    frequency=frequency,
                    units=units,
                    domain_id=domain_id,
                    dataset_id=dataset_id,
                ),
                "entity_type": "series",
                "domain_id": domain_id,
                "domain_name": domain_name,
                "dataset_id": dataset_id,
                "dataset_label": dataset_label,
                "title_pt": title_pt,
                "frequency": frequency,
                "units": units,
                "start_date": start_date,
                "end_date": end_date,
                "last_update": last_update,
                "source": "bpstat",
            }
        )

    return rows


@enumerator(output=BDP_ENUMERATE_OUTPUT, tags=["macro", "pt"])
async def enumerate_bdp(params: BdpEnumerateParams) -> pd.DataFrame:
    """Enumerate every Banco de Portugal series with parent context.

    Pipeline:

    1. ``GET /domains/`` — emit one ``entity_type='domain'`` row per leaf
       domain (``has_series=true``, ~65 of 77).
    2. For each leaf domain, ``GET /domains/{domain_id}/datasets/`` to
       discover datasets; emit one ``entity_type='dataset'`` row per
       dataset (~215 in total).
    3. For each dataset, walk paginated ``GET /domains/{d}/datasets/{ds}/``
       (10 series per page; ``next_page`` cursor) and emit one
       ``entity_type='series'`` row per discovered series (~72 K total).

    The dataset detail endpoint is paginated upstream at a fixed 10
    series/page; ``limit`` and ``obs_since=2099`` query params do *not*
    suppress observation traffic without dropping the series stubs as
    well, so we pay the bandwidth cost (~40 KB/page) and discard ``value``
    after parsing. At concurrency=4 with a 0.25 s inter-request delay,
    enumeration completes in ~15–20 minutes.

    Concurrency is capped at 4; 403/429/5xx retry 3× with exponential
    backoff and ``Retry-After`` honoring. After exhausting retries the
    affected dataset is skipped with a WARNING.
    """
    del params

    semaphore = asyncio.Semaphore(_METADATA_CONCURRENCY)
    rows: list[dict[str, str]] = []
    failed_datasets: list[str] = []

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0),
        headers=_HEADERS,
        follow_redirects=True,
    ) as client:
        domains = await _list_domains(client, semaphore)
        if not domains:
            logger.warning("BdP enumerate: /domains fetch failed; emitting empty catalog")
            return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))

        leaf_domains = [d for d in domains if d.get("has_series")]
        logger.info(
            "BdP enumerate: %d total domains, %d leaf domains (has_series=true)",
            len(domains),
            len(leaf_domains),
        )

        # Emit domain-level synthetic rows up-front.
        for d in leaf_domains:
            did = str(d.get("id", "")).strip()
            if not did:
                continue
            name = str(d.get("label") or d.get("description") or did).strip()
            description = str(d.get("description") or "").strip()
            num_series = int(d.get("num_series") or 0)
            num_datasets = int(d.get("num_datasets") or 0)
            rows.append(
                {
                    "code": f"domain:{did}",
                    "title": name,
                    "description": _domain_description(
                        name=name,
                        description=description,
                        num_series=num_series,
                        num_datasets=num_datasets,
                    ),
                    "entity_type": "domain",
                    "domain_id": did,
                    "domain_name": name,
                    "dataset_id": "",
                    "dataset_label": "",
                    "title_pt": "",
                    "frequency": "",
                    "units": "",
                    "start_date": "",
                    "end_date": "",
                    "last_update": str(d.get("obs_updated_at") or ""),
                    "source": "bpstat",
                }
            )

        # Discover datasets per leaf domain (concurrent fan-out).
        async def _discover(domain: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            did = domain.get("id", "")
            stubs = await _list_datasets(client, did, semaphore)
            return domain, stubs

        domain_results = await asyncio.gather(*[_discover(d) for d in leaf_domains])

        # Flatten to a list of (domain, dataset_stub) work items so we
        # can run the per-dataset crawl concurrency-capped without
        # nesting semaphores.
        work_items: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for domain, stubs in domain_results:
            for stub in stubs:
                work_items.append((domain, stub))

        logger.info("BdP enumerate: discovered %d datasets across leaf domains", len(work_items))

        # Per-dataset crawl. Each task internally serializes the page walk
        # but the global semaphore caps total in-flight HTTP at 4.
        async def _crawl_one(
            domain: dict[str, Any],
            stub: dict[str, Any],
        ) -> list[dict[str, str]]:
            did = str(domain.get("id", "")).strip()
            domain_name = str(domain.get("label") or domain.get("description") or did).strip()
            ext = stub.get("extension") if isinstance(stub.get("extension"), dict) else {}
            dataset_id = str(ext.get("id") or "").strip()
            if not dataset_id:
                return []
            dataset_label = str(stub.get("label") or ext.get("label") or dataset_id).strip()
            last_update = str(ext.get("obs_updated_at") or "")

            series_stubs, first_payload = await _crawl_dataset_series(
                client, did, dataset_id, semaphore
            )
            if first_payload is None:
                failed_datasets.append(f"{did}/{dataset_id}")
                return []

            # Optional PT label sweep — bulked at 100 IDs/call. We disable
            # this when the dataset is huge (>1000 series) to keep the
            # per-dataset request count bounded; PT labels for those
            # datasets will simply be empty (English title still works).
            pt_labels: dict[str, str] = {}
            if 0 < len(series_stubs) <= 1000:
                pt_labels = await _fetch_pt_labels(
                    client,
                    [str(s.get("id")) for s in series_stubs if s.get("id")],
                    semaphore,
                )

            return _emit_rows_for_dataset(
                domain_id=did,
                domain_name=domain_name,
                dataset_id=dataset_id,
                dataset_label=dataset_label,
                dataset_payload=first_payload,
                series_stubs=series_stubs,
                pt_labels=pt_labels,
                last_update=last_update,
            )

        per_dataset_rows = await asyncio.gather(*[_crawl_one(d, s) for d, s in work_items])
        for batch in per_dataset_rows:
            rows.extend(batch)

    if failed_datasets:
        logger.info(
            "BdP enumerate: %d datasets failed: %s",
            len(failed_datasets),
            ", ".join(failed_datasets[:20]),
        )
    else:
        logger.info("BdP enumerate: emitted %d rows", len(rows))

    columns = list(_ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_bdp.search import (  # noqa: E402  (after public decorators)
    BDP_SEARCH_OUTPUT,
    PARSIMONY_BDP_CATALOG_URL_ENV,
    BdpSearchParams,
    bdp_search,
)

CATALOGS: list[tuple[str, object]] = [("bdp", enumerate_bdp)]

CONNECTORS = Connectors([bdp_fetch, enumerate_bdp, bdp_search])

__all__ = [
    "BDP_ENUMERATE_OUTPUT",
    "BDP_FETCH_OUTPUT",
    "BDP_SEARCH_OUTPUT",
    "CATALOGS",
    "CONNECTORS",
    "BdpEnumerateParams",
    "BdpFetchParams",
    "BdpSearchParams",
    "PARSIMONY_BDP_CATALOG_URL_ENV",
    "bdp_fetch",
    "bdp_search",
    "enumerate_bdp",
]
