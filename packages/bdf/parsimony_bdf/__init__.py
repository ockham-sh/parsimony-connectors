"""Banque de France (BdF): fetch + catalog enumeration.

API base: ``https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets``
(Webstat Opendatasoft public API). Requires a free API key via the
``BANQUEDEFRANCE_KEY`` environment variable, sent in the
``Authorization: Apikey <KEY>`` header (literal word ``Apikey`` — *not*
``Bearer``). Register at https://developer.webstat.banque-france.fr/.

The catalog enumerator is series-grained — every individual time series
across BdF's 45 datasets (~41,607 series total) is published as its own
row, alongside synthetic ``dataset:`` parent stub rows so agents can
navigate from search hits to their parent dataset context.

Endpoints used:

* ``GET /webstat-datasets/exports/json`` — list of 45 datasets in a
  single response (no pagination).
* ``GET /series/exports/json?refine=dataset_id:{ID}`` — list of every
  series in a given dataset, in a single response. Each row carries
  ``series_key``, multilingual titles, time bounds, source agency and a
  JSON-encoded ``series_dimensions_and_values`` dict.
* ``GET /observations/exports/json?where=series_key="{KEY}"`` — fetch
  observations for a single series. Used by :func:`bdf_fetch`.

Quota: 10,000 requests/day. Updates at 9am/1pm/9pm Paris. Full publish
costs ~46 requests (1 dataset list + 45 series listings); well within
the daily cap.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Annotated, Any

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


_BASE_URL = "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets"

_ENV: dict[str, str] = {"api_key": "BANQUEDEFRANCE_KEY"}

# Conservative throttling. The Opendatasoft endpoint is rate-limited
# globally at 10K requests/day and per-IP at a modest QPS; concurrency=4
# with a 0.25s inter-request delay keeps enumeration smooth without
# tripping the WAF.
_METADATA_CONCURRENCY = 4
_INTER_REQUEST_DELAY_S = 0.25
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_RETRY_BACKOFFS_S: tuple[float, ...] = (1.0, 2.0, 4.0)

# Cap descriptions before they reach the embedder. BdF titles are short,
# but bilingual concatenation can expand the description meaningfully —
# a hard cap keeps the embedder context-window-safe.
_DESCRIPTION_CHAR_CAP = 1500

# Series payloads can be 1.6MB+ (the full series listing for big
# datasets); allow long reads.
_HTTP_TIMEOUT = httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0)


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BdfFetchParams(BaseModel):
    """Parameters for fetching Banque de France time series."""

    key: Annotated[str, "ns:bdf"] = Field(
        ...,
        description=(
            "Dot-separated SDMX series key as published by BdF Webstat "
            "(e.g. EXR.D.USD.EUR.SP00.A or ICP.M.FR.N.000000.4.ANR). "
            "Discover keys via bdf_search or enumerate_bdf."
        ),
    )
    start_period: str | None = Field(
        default=None,
        description="Start period (YYYY-MM-DD); filters time_period_start.",
    )
    end_period: str | None = Field(
        default=None,
        description="End period (YYYY-MM-DD); filters time_period_start.",
    )

    @field_validator("key")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("key must be non-empty")
        return v


class BdfEnumerateParams(BaseModel):
    """No parameters needed — enumerates BdF datasets and series."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BDF_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # KEY shape:
        # * dataset rows — ``"dataset:{dataset_id}"`` (45 of these).
        # * series rows  — the raw ``series_key`` (already globally unique
        #                  e.g. ``EXR.M.USD.EUR.SP00.A``); not prefixed.
        # The synthetic ``dataset:`` prefix mirrors BoJ's ``db:`` and BdP's
        # ``dataset:`` so downstream consumers can split entity types by
        # KEY alone (or by the ``entity_type`` METADATA column).
        Column(name="code", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.DESCRIPTION),
        Column(name="entity_type", role=ColumnRole.METADATA),  # "dataset" | "series"
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(name="dataset_description", role=ColumnRole.METADATA),
        Column(name="series_key", role=ColumnRole.METADATA),
        Column(name="title_fr", role=ColumnRole.METADATA),
        Column(name="title_long_en", role=ColumnRole.METADATA),
        Column(name="title_long_fr", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="ref_area", role=ColumnRole.METADATA),
        Column(name="first_time_period", role=ColumnRole.METADATA),
        Column(name="last_time_period", role=ColumnRole.METADATA),
        Column(name="source_agency", role=ColumnRole.METADATA),
        Column(name="dimensions_json", role=ColumnRole.METADATA),
    ]
)

BDF_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, param_key="key", namespace="bdf"),
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
    "dataset_id",
    "dataset_description",
    "series_key",
    "title_fr",
    "title_long_en",
    "title_long_fr",
    "frequency",
    "ref_area",
    "first_time_period",
    "last_time_period",
    "source_agency",
    "dimensions_json",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, cap: int = _DESCRIPTION_CHAR_CAP) -> str:
    """Cap a string at ``cap`` chars; return as-is if shorter."""
    if not text:
        return ""
    if len(text) <= cap:
        return text
    return text[:cap].rstrip()


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header; ``None`` if absent/malformed."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _auth_headers(api_key: str) -> dict[str, str]:
    """Build the BdF Webstat Opendatasoft auth + transport headers.

    Note the literal ``Apikey`` token (not ``Bearer``) — Opendatasoft's
    auth scheme is non-standard.
    """
    return {
        "Authorization": f"Apikey {api_key}",
        "Accept": "application/json",
        "User-Agent": "parsimony-bdf/0.1",
    }


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    semaphore: asyncio.Semaphore,
    params: dict[str, str] | None = None,
) -> Any | None:
    """GET ``url`` and return the parsed JSON body.

    Retries 429/5xx with exponential backoff and honors ``Retry-After``.
    On exhausted retries / network errors / non-JSON bodies, logs a
    WARNING and returns ``None`` so the caller can decide whether to
    skip.
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
                    logger.warning("BdF %s returned non-JSON body: %s", url, exc)
                    return None

            last_status = response.status_code
            if response.status_code in _RETRY_STATUSES and backoff is not None:
                wait = _retry_after_seconds(response) or backoff
                logger.info(
                    "BdF %s returned %s (attempt %d); retrying in %.1fs",
                    url,
                    response.status_code,
                    attempt + 1,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            break

        logger.warning(
            "BdF fetch failed for %s after retries (last_status=%s, last_error=%s)",
            url,
            last_status,
            last_error,
        )
        return None


def _parse_dimensions(raw: str | None) -> dict[str, Any]:
    """Decode the ``series_dimensions_and_values`` JSON-string field.

    BdF emits the dimensions dict as a JSON-encoded *string* (not a
    nested object) in the JSON response. Returns an empty dict on
    missing / malformed input.
    """
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    if not isinstance(decoded, dict):
        return {}
    return decoded


def _dataset_title(dataset_id: str, description_en: str) -> str:
    """Pick the dataset row's TITLE: dataset_id, or description_en capped."""
    desc = (description_en or "").strip()
    if desc:
        return desc[:90]
    return dataset_id


def _dataset_description(*, description_en: str, description_fr: str) -> str:
    """Combine EN + FR descriptions for the embedder, deduping if equal."""
    en = (description_en or "").strip()
    fr = (description_fr or "").strip()
    if en and fr:
        if en.lower() == fr.lower():
            return _truncate(en)
        return _truncate(f"{en} | {fr}")
    return _truncate(en or fr)


def _series_title(
    *,
    title_en: str,
    title_fr: str,
    title_long_en: str,
    title_long_fr: str,
    series_key: str,
) -> str:
    """Pick the most informative non-empty title for a series row."""
    for candidate in (title_en, title_fr, title_long_en, title_long_fr):
        cand = (candidate or "").strip()
        if cand:
            return cand
    return series_key


def _series_description(
    *,
    title_en: str,
    title_fr: str,
    title_long_en: str,
    title_long_fr: str,
    dataset_id: str,
    dataset_description: str,
    source_agency: str,
) -> str:
    """Build a bilingual series description for the embedder.

    Folds EN + FR titles plus dataset context into one string so the
    multilingual embedder sees both languages and the parent dataset's
    semantic context. EN/FR halves are deduped when identical.
    """
    en_part = (title_long_en or title_en or "").strip()
    fr_part = (title_long_fr or title_fr or "").strip()

    bilingual: str
    if en_part and fr_part:
        if en_part.lower() == fr_part.lower():
            bilingual = en_part
        else:
            bilingual = f"{en_part} | {fr_part}"
    else:
        bilingual = en_part or fr_part

    chunks: list[str] = []
    if bilingual:
        chunks.append(bilingual)
    if dataset_id:
        ds_ctx = f"Dataset: {dataset_id}"
        if dataset_description:
            ds_ctx += f" ({dataset_description})"
        ds_ctx += "."
        chunks.append(ds_ctx)
    if source_agency:
        chunks.append(f"Source: {source_agency}.")
    return _truncate(" | ".join(c for c in chunks if c).strip())


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(env=_ENV, output=BDF_FETCH_OUTPUT, tags=["macro", "fr"])
async def bdf_fetch(params: BdfFetchParams, *, api_key: str) -> Result:
    """Fetch Banque de France time series via the Webstat Opendatasoft API.

    Pulls observation rows for a single series key and returns
    ``(key, title, date, value)`` rows. Optional ``start_period`` /
    ``end_period`` filter on ``time_period_start``.
    """
    headers = _auth_headers(api_key)
    where = f'series_key="{params.key}"'
    if params.start_period:
        where += f" and time_period_start>=date'{params.start_period}'"
    if params.end_period:
        where += f" and time_period_start<=date'{params.end_period}'"

    req_params: dict[str, str] = {
        "select": (
            "series_key,title_en,title_fr,time_period,"
            "time_period_start,time_period_end,obs_value,obs_status"
        ),
        "where": where,
        "order_by": "time_period_start",
    }

    url = f"{_BASE_URL}/observations/exports/json"

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, headers=headers) as client:
        response = await client.get(url, params=req_params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="bdf", op_name="observations")
        try:
            payload = response.json()
        except ValueError as exc:
            raise EmptyDataError(
                provider="bdf",
                message=f"BdF returned non-JSON body for key={params.key}: {exc}",
            ) from exc

    if not isinstance(payload, list) or not payload:
        raise EmptyDataError(provider="bdf", message=f"No data returned for key: {params.key}")

    rows: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        date_str = row.get("time_period_start") or row.get("time_period") or ""
        if not date_str:
            continue
        raw_value = row.get("obs_value")
        try:
            value = float(raw_value) if raw_value is not None else None
        except (ValueError, TypeError):
            value = None
        title = (
            row.get("title_en")
            or row.get("title_fr")
            or row.get("series_key")
            or params.key
        )
        rows.append(
            {
                "key": str(row.get("series_key") or params.key),
                "title": str(title),
                "date": str(date_str),
                "value": value,
            }
        )

    if not rows:
        raise EmptyDataError(
            provider="bdf",
            message=f"No observations parsed for key: {params.key}",
        )

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="bdf",
            params={"key": params.key},
            properties={"source_url": "https://webstat.banque-france.fr"},
        ),
    )


# ---------------------------------------------------------------------------
# Enumerator helpers
# ---------------------------------------------------------------------------


async def _list_datasets(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Return every BdF dataset (45 entries) in a single request."""
    url = f"{_BASE_URL}/webstat-datasets/exports/json"
    params = {
        "select": (
            "dataset_id,description_en,description_fr,"
            "series_count,last_observation_date"
        ),
        "order_by": "dataset_id",
    }
    payload = await _get_json(client, url, params=params, semaphore=semaphore)
    if not isinstance(payload, list):
        return []
    return [d for d in payload if isinstance(d, dict)]


async def _list_series(
    client: httpx.AsyncClient,
    dataset_id: str,
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]] | None:
    """Return every series row for ``dataset_id``.

    The Webstat Opendatasoft series export returns the full listing in
    one response (no pagination needed, even for the largest datasets).
    Returns ``None`` on transport / parse failure so the caller can
    decide to skip the dataset's series rows but still emit the dataset
    stub.
    """
    url = f"{_BASE_URL}/series/exports/json"
    params = {
        "select": (
            "series_key,title_fr,title_en,title_long_fr,title_long_en,"
            "first_time_period_date,last_time_period_date,source_agency,"
            "series_dimensions_and_values"
        ),
        "refine": f"dataset_id:{dataset_id}",
    }
    payload = await _get_json(client, url, params=params, semaphore=semaphore)
    if payload is None:
        return None
    if not isinstance(payload, list):
        return []
    return [s for s in payload if isinstance(s, dict)]


def _emit_rows_for_dataset(
    *,
    dataset: dict[str, Any],
    series_rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Build catalog rows for a single dataset (1 stub + N series)."""
    dataset_id = str(dataset.get("dataset_id") or "").strip()
    if not dataset_id:
        return []
    description_en = str(dataset.get("description_en") or "").strip()
    description_fr = str(dataset.get("description_fr") or "").strip()
    dataset_desc_canonical = description_en or description_fr

    rows: list[dict[str, str]] = []

    # Dataset stub row.
    rows.append(
        {
            "code": f"dataset:{dataset_id}",
            "title": _dataset_title(dataset_id, description_en),
            "description": _dataset_description(
                description_en=description_en,
                description_fr=description_fr,
            ),
            "entity_type": "dataset",
            "dataset_id": dataset_id,
            "dataset_description": dataset_desc_canonical,
            "series_key": "",
            "title_fr": "",
            "title_long_en": "",
            "title_long_fr": "",
            "frequency": "",
            "ref_area": "",
            "first_time_period": "",
            "last_time_period": "",
            "source_agency": "",
            "dimensions_json": "",
        }
    )

    # Series rows.
    for series in series_rows:
        series_key = str(series.get("series_key") or "").strip()
        if not series_key:
            continue
        title_en = str(series.get("title_en") or "").strip()
        title_fr = str(series.get("title_fr") or "").strip()
        title_long_en = str(series.get("title_long_en") or "").strip()
        title_long_fr = str(series.get("title_long_fr") or "").strip()
        first_period = str(series.get("first_time_period_date") or "").strip()
        last_period = str(series.get("last_time_period_date") or "").strip()
        source_agency = str(series.get("source_agency") or "").strip()
        dims_raw = series.get("series_dimensions_and_values")
        dims_str = dims_raw if isinstance(dims_raw, str) else ""
        dims = _parse_dimensions(dims_str)
        frequency = str(dims.get("FREQ") or "").strip()
        ref_area = str(dims.get("REF_AREA") or "").strip()

        rows.append(
            {
                "code": series_key,
                "title": _series_title(
                    title_en=title_en,
                    title_fr=title_fr,
                    title_long_en=title_long_en,
                    title_long_fr=title_long_fr,
                    series_key=series_key,
                ),
                "description": _series_description(
                    title_en=title_en,
                    title_fr=title_fr,
                    title_long_en=title_long_en,
                    title_long_fr=title_long_fr,
                    dataset_id=dataset_id,
                    dataset_description=dataset_desc_canonical,
                    source_agency=source_agency,
                ),
                "entity_type": "series",
                "dataset_id": dataset_id,
                "dataset_description": dataset_desc_canonical,
                "series_key": series_key,
                "title_fr": title_fr,
                "title_long_en": title_long_en,
                "title_long_fr": title_long_fr,
                "frequency": frequency,
                "ref_area": ref_area,
                "first_time_period": first_period,
                "last_time_period": last_period,
                "source_agency": source_agency,
                "dimensions_json": dims_str,
            }
        )

    return rows


@enumerator(env=_ENV, output=BDF_ENUMERATE_OUTPUT, tags=["macro", "fr"])
async def enumerate_bdf(params: BdfEnumerateParams, *, api_key: str) -> pd.DataFrame:
    """Enumerate every BdF series with parent dataset context.

    Pipeline:

    1. ``GET /webstat-datasets/exports/json`` — pull all 45 datasets in
       a single request.
    2. For each dataset, ``GET /series/exports/json?refine=dataset_id:{ID}``
       — pull all series for that dataset (full listing in one
       response).
    3. Emit one ``entity_type='dataset'`` stub per dataset (~45) and one
       ``entity_type='series'`` row per discovered series (~41,607).

    Concurrency is capped at 4 with a 0.25 s inter-request delay; 429/
    5xx responses retry up to 3 times with exponential backoff and
    honor ``Retry-After``. After exhausting retries the affected
    dataset's series rows are skipped (the dataset stub is still
    emitted) with a WARNING.

    Cost: ~46 requests total (well under the 10K/day quota).
    """
    del params

    semaphore = asyncio.Semaphore(_METADATA_CONCURRENCY)
    rows: list[dict[str, str]] = []
    failed_datasets: list[str] = []

    async with httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT,
        headers=_auth_headers(api_key),
        follow_redirects=True,
    ) as client:
        datasets = await _list_datasets(client, semaphore)
        if not datasets:
            logger.warning("BdF enumerate: dataset list fetch failed; emitting empty catalog")
            return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))

        logger.info("BdF enumerate: discovered %d datasets", len(datasets))

        async def _crawl_one(dataset: dict[str, Any]) -> list[dict[str, str]]:
            dataset_id = str(dataset.get("dataset_id") or "").strip()
            if not dataset_id:
                return []
            series_rows = await _list_series(client, dataset_id, semaphore)
            if series_rows is None:
                # Transport failure — emit dataset stub only, log warning.
                failed_datasets.append(dataset_id)
                series_rows = []
            return _emit_rows_for_dataset(dataset=dataset, series_rows=series_rows)

        per_dataset_rows = await asyncio.gather(*[_crawl_one(d) for d in datasets])
        for batch in per_dataset_rows:
            rows.extend(batch)

    if failed_datasets:
        logger.warning(
            "BdF enumerate: %d datasets failed series fetch (stubs emitted, series omitted): %s",
            len(failed_datasets),
            ", ".join(failed_datasets[:20]),
        )
    else:
        logger.info("BdF enumerate: emitted %d rows", len(rows))

    columns = list(_ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_bdf.search import (  # noqa: E402  (after public decorators)
    BDF_SEARCH_OUTPUT,
    PARSIMONY_BDF_CATALOG_URL_ENV,
    BdfSearchParams,
    bdf_search,
)

CATALOGS: list[tuple[str, object]] = [("bdf", enumerate_bdf)]

CONNECTORS = Connectors([bdf_fetch, enumerate_bdf, bdf_search])

__all__ = [
    "BDF_ENUMERATE_OUTPUT",
    "BDF_FETCH_OUTPUT",
    "BDF_SEARCH_OUTPUT",
    "CATALOGS",
    "CONNECTORS",
    "BdfEnumerateParams",
    "BdfFetchParams",
    "BdfSearchParams",
    "PARSIMONY_BDF_CATALOG_URL_ENV",
    "bdf_fetch",
    "bdf_search",
    "enumerate_bdf",
]
