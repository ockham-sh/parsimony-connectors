"""Bank of Japan (BoJ): fetch + catalog enumeration.

API manual: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf
No authentication required. Max 250 codes per request.

"""

from __future__ import annotations

import asyncio
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


_BASE_URL = "https://www.stat-search.boj.or.jp/api/v1"

# BoJ's stat_search endpoints sit behind Akamai, which blocks both the
# default httpx User-Agent and high-concurrency fan-outs. Empirically a
# concurrency cap of 2, a small inter-request delay, and a browser UA
# are enough to keep enumeration stable; higher concurrency triggers 403s.
_METADATA_CONCURRENCY = 2
_INTER_REQUEST_DELAY_S = 0.5
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_RETRY_STATUSES = frozenset({403, 429, 500, 502, 503, 504})
_RETRY_BACKOFFS_S: tuple[float, ...] = (1.0, 2.0, 4.0)

# Frequency tokens emitted by BoJ metadata. Anything not in the map passes
# through unchanged so we never silently corrupt an unknown frequency.
_FREQ_MAP: dict[str, str] = {
    "DAILY": "Daily",
    "DM": "Daily",
    "WEEKLY": "Weekly",
    "WEEKLY(MON)": "Weekly (Mon)",
    "WEEKLY(THU)": "Weekly (Thu)",
    "MONTHLY": "Monthly",
    "MM": "Monthly",
    "QUARTERLY": "Quarterly",
    "QM": "Quarterly",
    "SEMI-ANNUAL": "Semi-annual",
    "SEMIANNUAL": "Semi-annual",
    "SM": "Semi-annual",
    "ANNUAL": "Annual",
    "ANNUAL(MAR)": "Annual (Mar)",
    "AM": "Annual",
}


# Canonical BoJ database catalog. Source:
# https://www.stat-search.boj.or.jp/info/api_manual.pdf
# Cross-validated against pyboj and boj-stat-search-python (both verified
# against the official PDF). Update when the official manual changes.
# Tuple shape: ``(code, category, title)``.
_BOJ_DATABASES: tuple[tuple[str, str, str], ...] = (
    ("IR01", "Interest Rates on Deposits and Loans", "Basic Discount Rate and Basic Loan Rate"),
    (
        "IR02",
        "Interest Rates on Deposits and Loans",
        "Average Interest Rates Posted at Financial Institutions by Type of Deposit",
    ),
    ("IR03", "Interest Rates on Deposits and Loans", "Average Interest Rates on Time Deposits by Term"),
    ("IR04", "Interest Rates on Deposits and Loans", "Average Contract Interest Rates on Loans and Discounts"),
    ("FM01", "Financial Markets", "Uncollateralized Overnight Call Rate (Updated every business day)"),
    ("FM02", "Financial Markets", "Short-term Money Market Rates"),
    ("FM03", "Financial Markets", "Amounts Outstanding in Short-term Money Market"),
    ("FM04", "Financial Markets", "Amounts Outstanding in the Call Money Market"),
    ("FM05", "Financial Markets", "Issuance, Redemption, and Outstanding of Public and Corporate Bonds"),
    ("FM06", "Financial Markets", "Trading of Interest-bearing Government Bonds by Purchaser"),
    (
        "FM07",
        "Financial Markets",
        "(Reference) Government Bonds Sales Over the Counter / Counter Sales Ratio",
    ),
    ("FM08", "Financial Markets", "Foreign Exchange Rates"),
    ("FM09", "Financial Markets", "Effective Exchange Rate"),
    ("PS01", "Payment and Settlement", "Other Payment and Settlement Systems"),
    ("PS02", "Payment and Settlement", "Basic Figures on Fails"),
    ("MD01", "Money, Deposits and Loans", "Monetary Base"),
    ("MD02", "Money, Deposits and Loans", "Money Stock"),
    ("MD03", "Money, Deposits and Loans", "Monetary Survey"),
    ("MD04", "Money, Deposits and Loans", "(Reference) Changes in Money Stock (M2+CDs) and Credit"),
    ("MD05", "Money, Deposits and Loans", "Currency in Circulation"),
    (
        "MD06",
        "Money, Deposits and Loans",
        "Sources of Changes in Current Account Balances at the BOJ and Market Operations",
    ),
    ("MD07", "Money, Deposits and Loans", "Reserves"),
    ("MD08", "Money, Deposits and Loans", "BOJ Current Account Balances by Sector"),
    ("MD09", "Money, Deposits and Loans", "Monetary Base and the Bank of Japan's Transactions"),
    ("MD10", "Money, Deposits and Loans", "Amounts Outstanding of Deposits by Depositor"),
    ("MD11", "Money, Deposits and Loans", "Deposits, Vault Cash, and Loans and Bills Discounted"),
    (
        "MD12",
        "Money, Deposits and Loans",
        "Deposits, Vault Cash, and Loans and Bills Discounted by Prefecture",
    ),
    ("MD13", "Money, Deposits and Loans", "Principal Figures of Financial Institutions"),
    ("MD14", "Money, Deposits and Loans", "Time Deposits: Amounts Outstanding and New Deposits by Maturity"),
    ("LA01", "Money, Deposits and Loans", "Loans and Bills Discounted by Sector"),
    ("LA02", "Money, Deposits and Loans", "Loans and Discounts by the Bank of Japan"),
    ("LA03", "Money, Deposits and Loans", "Outstanding of Loans (Others)"),
    ("LA04", "Money, Deposits and Loans", "Commitment Lines Extended by Japanese Banks"),
    (
        "LA05",
        "Money, Deposits and Loans",
        "Senior Loan Officer Opinion Survey on Bank Lending Practices",
    ),
    ("BS01", "Balance Sheets", "Bank of Japan Accounts"),
    ("BS02", "Balance Sheets", "Financial Institutions Accounts"),
    ("FF", "Flow of Funds", "Flow of Funds"),
    ("OB01", "Other Bank of Japan Statistics", "Bank of Japan's Transactions with the Government"),
    ("OB02", "Other Bank of Japan Statistics", "Collateral Accepted by the Bank of Japan"),
    ("CO", "TANKAN", "TANKAN (Short-term Economic Survey of Enterprises in Japan)"),
    ("PR01", "Prices", "Corporate Goods Price Index (CGPI)"),
    ("PR02", "Prices", "Services Producer Price Index (SPPI)"),
    (
        "PR03",
        "Prices",
        "Input-Output Price Index of the Manufacturing Industry by Sector (IOPI)",
    ),
    ("PR04", "Prices", "Final Demand-Intermediate Demand price indexes (FD-ID)"),
    ("PF01", "Public Finance", "Statement of Receipts and Payments of the Treasury Accounts"),
    ("PF02", "Public Finance", "National Government Debt"),
    ("BP01", "Balance of Payments and BIS-Related Statistics", "Balance of Payments"),
    (
        "BIS",
        "Balance of Payments and BIS-Related Statistics",
        "BIS International Locational Banking Statistics and Consolidated Banking Statistics in Japan",
    ),
    (
        "DER",
        "Balance of Payments and BIS-Related Statistics",
        "Regular Derivatives Market Statistics in Japan",
    ),
    ("OT", "Others", "Others"),
)


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BojFetchParams(BaseModel):
    """Parameters for fetching Bank of Japan time series."""

    db: str = Field(
        ...,
        description="Database code (e.g. FM08 for FX rates, PR01 for prices)",
    )
    code: Annotated[str, "ns:boj"] = Field(
        ...,
        description="Comma-separated series codes (max 250, e.g. FXERD01)",
    )
    start_date: str | None = Field(
        default=None,
        description="Start date (YYYYMMDD, YYYYMM, or YYYY depending on frequency)",
    )
    end_date: str | None = Field(
        default=None,
        description="End date (same format as start_date)",
    )
    lang: str = Field(default="en", description="Language: en or jp")

    @field_validator("db")
    @classmethod
    def _non_empty_db(cls, v: str) -> str:
        v = v.strip().upper()
        if not v:
            raise ValueError("db must be non-empty")
        return v

    @field_validator("code")
    @classmethod
    def _validate_codes(cls, v: str) -> str:
        codes = [s.strip() for s in v.split(",") if s.strip()]
        if not codes:
            raise ValueError("At least one series code required")
        if len(codes) > 250:
            raise ValueError("Maximum 250 codes per request")
        return ",".join(codes)


class BojEnumerateParams(BaseModel):
    """No parameters needed — enumerates BoJ statistics databases."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BOJ_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # KEY: series code (e.g. ``STRDCLUCON``) for series rows, or
        # ``db:<code>`` for DB-level rows. The ``db:`` prefix mirrors BoC's
        # ``group:`` synthetic keys so agents and downstream consumers can
        # distinguish DB-level catalog entries from series rows by KEY
        # alone.
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        # TITLE: ``NAME_OF_TIME_SERIES`` for series rows; canonical DB title
        # for DB rows.
        Column(name="title", role=ColumnRole.TITLE),
        # DESCRIPTION feeds the embedder via ``semantic_text()`` — concat of
        # breadcrumb + category + unit + frequency + parent DB title (and
        # NOTES if present) for series; a short summary for DB rows.
        Column(name="description", role=ColumnRole.DESCRIPTION),
        # METADATA columns (filtering / dispatch / UI hints):
        Column(name="db", role=ColumnRole.METADATA),
        Column(name="db_title", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),  # "series" | "db"
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="breadcrumb", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
        Column(name="last_update", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),  # constant "stat_search"
    ]
)

BOJ_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, param_key="code", namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


_ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "db",
    "db_title",
    "entity_type",
    "frequency",
    "unit",
    "category",
    "breadcrumb",
    "start_date",
    "end_date",
    "last_update",
    "source",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_boj_date(date_str: str, freq: str) -> str:
    """Parse BoJ date string based on frequency code."""
    freq_lower = freq.lower()
    if freq_lower in ("dm", "daily"):
        # YYYYMMDD
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    elif freq_lower in ("mm", "monthly"):
        # YYYYMM
        if len(date_str) >= 6:
            return f"{date_str[:4]}-{date_str[4:6]}-01"
    elif freq_lower in ("qm", "quarterly"):
        # YYYYQQ where QQ is 01-04
        if len(date_str) >= 6:
            quarter = int(date_str[4:6])
            month = (quarter - 1) * 3 + 1
            return f"{date_str[:4]}-{month:02d}-01"
    elif freq_lower in ("am", "annual") and len(date_str) >= 4:
        return f"{date_str[:4]}-01-01"
    return date_str


def _normalize_frequency(raw: str) -> str:
    """Normalize a BoJ frequency token to plain English title-case.

    Unknown tokens pass through unchanged so we never silently mask an
    unfamiliar value.
    """
    if not raw:
        return ""
    return _FREQ_MAP.get(raw.strip().upper(), raw)


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header; return ``None`` if absent/malformed.

    Only seconds-format values are honored (HTTP-date format is rare in
    Akamai responses and not worth the complexity).
    """
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


async def _fetch_metadata(
    client: httpx.AsyncClient,
    db: str,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any] | None:
    """Fetch one DB's metadata, retrying transient Akamai blocks.

    Returns the parsed JSON body on success, ``None`` after exhausting
    retries. Failures emit a WARNING — the orchestrator wants visibility,
    not silent skips.
    """
    url = f"{_BASE_URL}/getMetadata"
    params = {"db": db, "lang": "en"}

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
                    payload: dict[str, Any] = response.json()
                except ValueError as exc:
                    logger.warning("BoJ metadata for %s returned non-JSON body: %s", db, exc)
                    return None
                return payload

            last_status = response.status_code
            if response.status_code in _RETRY_STATUSES and backoff is not None:
                wait = _retry_after_seconds(response) or backoff
                logger.info(
                    "BoJ metadata %s returned %s (attempt %d); retrying in %.1fs",
                    db,
                    response.status_code,
                    attempt + 1,
                    wait,
                )
                await asyncio.sleep(wait)
                continue
            # Non-retriable status (e.g. 404) — give up immediately.
            break

        logger.warning(
            "BoJ metadata fetch failed for db=%s after retries (last_status=%s, last_error=%s)",
            db,
            last_status,
            last_error,
        )
        return None


def _layers(series_row: dict[str, Any]) -> list[tuple[int, str]]:
    """Extract non-empty layer entries as ``(layer_number, title)`` tuples.

    BoJ occasionally encodes layer values as numeric JSON literals (e.g. an
    integer year like ``2024`` for time-series snapshots), so we coerce to
    str defensively before stripping.
    """
    out: list[tuple[int, str]] = []
    for i in range(1, 6):
        raw = series_row.get(f"LAYER{i}")
        if raw is None:
            continue
        title = str(raw).strip()
        if title:
            out.append((i, title))
    return out


def _build_breadcrumb(layer_stack: dict[int, str]) -> str:
    """Render the active layer stack as a breadcrumb string.

    ``layer_stack`` maps layer number → header title for currently-open
    sections. The breadcrumb walks layers in depth order.
    """
    parts = [layer_stack[k] for k in sorted(layer_stack)]
    return " > ".join(parts)


def _series_description(
    *,
    breadcrumb: str,
    category: str,
    unit: str,
    frequency: str,
    db_code: str,
    db_title: str,
    notes: str,
) -> str:
    """Assemble the per-series DESCRIPTION text fed to the embedder."""
    chunks: list[str] = []
    if breadcrumb:
        chunks.append(f"{breadcrumb}.")
    if category:
        chunks.append(f"{category}.")
    if unit:
        chunks.append(f"Unit: {unit}.")
    if frequency:
        chunks.append(f"{frequency} frequency.")
    chunks.append(f"Bank of Japan {db_title} (db={db_code}).")
    if notes:
        chunks.append(notes.strip())
    return " ".join(c for c in chunks if c).strip()


def _db_description(
    *,
    category: str,
    n_series: int,
    db_code: str,
    top_sections: list[str],
) -> str:
    """Assemble the DB-level DESCRIPTION text."""
    top = ", ".join(top_sections[:5])
    parts = [
        "Bank of Japan statistics database.",
        f"Category: {category}." if category else "",
        f"Covers {n_series} series.",
        f"Top-level sections: {top}." if top else "",
        f"Fetch via boj_fetch(db='{db_code}', code=...).",
    ]
    return " ".join(p for p in parts if p).strip()


def _emit_rows_for_db(
    *,
    db_code: str,
    db_title: str,
    db_category: str,
    metadata: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Convert a single DB's metadata payload into catalog rows.

    Always emits a DB-level row. Series rows come from ``RESULTSET``
    entries that carry a non-empty ``SERIES_CODE``; layer headers (rows
    with empty ``SERIES_CODE``) are used to build breadcrumbs.
    """
    rows: list[dict[str, str]] = []
    layer_stack: dict[int, str] = {}
    n_series = 0
    top_sections: list[str] = []
    seen_top: set[str] = set()

    result_set = (metadata or {}).get("RESULTSET") or []
    if not isinstance(result_set, list):
        result_set = []

    for series in result_set:
        if not isinstance(series, dict):
            continue
        series_code = (series.get("SERIES_CODE") or "").strip()
        if not series_code:
            # Layer header row — refresh the stack for subsequent series.
            # When a layer updates, drop any strictly-deeper entries so
            # the breadcrumb reflects the current section path.
            layers = _layers(series)
            if not layers:
                continue
            shallowest = min(layer_num for layer_num, _ in layers)
            for deeper in [k for k in layer_stack if k >= shallowest]:
                layer_stack.pop(deeper, None)
            for layer_num, title in layers:
                layer_stack[layer_num] = title
            # Track top-level sections (LAYER1) for the DB description.
            top = layer_stack.get(1, "").strip()
            if top and top not in seen_top:
                seen_top.add(top)
                top_sections.append(top)
            continue

        # Series row — also propagate any layer hints embedded on the
        # series row itself (BoJ sometimes emits LAYER fields directly on
        # the series record without a preceding header row).
        for layer_num, title in _layers(series):
            layer_stack[layer_num] = title
        top = layer_stack.get(1, "").strip()
        if top and top not in seen_top:
            seen_top.add(top)
            top_sections.append(top)

        title = (
            series.get("NAME_OF_TIME_SERIES")
            or series.get("NAME_OF_TIME_SERIES_J")
            or series_code
        )
        unit = (series.get("UNIT") or "").strip()
        frequency = _normalize_frequency(series.get("FREQUENCY") or "")
        category = (series.get("CATEGORY") or db_category).strip()
        breadcrumb = _build_breadcrumb(layer_stack)
        notes = (series.get("NOTES") or "").strip()
        start = (series.get("START_OF_THE_TIME_SERIES") or "").strip()
        end = (series.get("END_OF_THE_TIME_SERIES") or "").strip()
        last_update = (series.get("LAST_UPDATE") or "").strip()

        description = _series_description(
            breadcrumb=breadcrumb,
            category=category,
            unit=unit,
            frequency=frequency,
            db_code=db_code,
            db_title=db_title,
            notes=notes,
        )

        rows.append(
            {
                "code": series_code,
                "title": str(title),
                "description": description,
                "db": db_code,
                "db_title": db_title,
                "entity_type": "series",
                "frequency": frequency,
                "unit": unit,
                "category": category,
                "breadcrumb": breadcrumb,
                "start_date": start,
                "end_date": end,
                "last_update": last_update,
                "source": "stat_search",
            }
        )
        n_series += 1

    rows.append(
        {
            "code": f"db:{db_code}",
            "title": db_title,
            "description": _db_description(
                category=db_category,
                n_series=n_series,
                db_code=db_code,
                top_sections=top_sections,
            ),
            "db": db_code,
            "db_title": db_title,
            "entity_type": "db",
            "frequency": "",
            "unit": "",
            "category": db_category,
            "breadcrumb": "",
            "start_date": "",
            "end_date": "",
            "last_update": "",
            "source": "stat_search",
        }
    )
    return rows


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BOJ_FETCH_OUTPUT, tags=["macro", "jp"])
async def boj_fetch(params: BojFetchParams) -> Result:
    """Fetch Bank of Japan time series by database and series code(s).

    Returns date + value with series metadata. Max 250 codes per request.
    """
    url = f"{_BASE_URL}/getDataCode"
    req_params: dict[str, str] = {
        "db": params.db,
        "code": params.code,
        "lang": params.lang,
    }
    if params.start_date:
        req_params["startDate"] = params.start_date
    if params.end_date:
        req_params["endDate"] = params.end_date

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(url, params=req_params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="boj", op_name="series")
        json_data = response.json()

    result_set = json_data.get("RESULTSET", [])
    if not result_set:
        raise EmptyDataError(provider="boj", message=f"No data returned for db={params.db}, code={params.code}")

    rows: list[dict[str, Any]] = []
    for series in result_set:
        code = series.get("SERIES_CODE", "")
        name = series.get("NAME_OF_TIME_SERIES", series.get("NAME_OF_TIME_SERIES_J", code))
        freq = series.get("FREQUENCY", "").lower()
        dates = series.get("VALUES", {}).get("SURVEY_DATES", [])
        values = series.get("VALUES", {}).get("VALUES", [])

        if isinstance(dates, str):
            dates = [dates]
        if isinstance(values, (str, int, float)):
            values = [values]

        for date_str, raw_value in zip(dates, values, strict=False):
            try:
                value = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                value = None
            if value is None:
                continue
            rows.append(
                {
                    "code": code,
                    "title": name,
                    "date": _parse_boj_date(str(date_str), freq),
                    "value": value,
                }
            )

    if not rows:
        raise EmptyDataError(provider="boj", message=f"No observations parsed for db={params.db}, code={params.code}")

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="boj",
            params={"db": params.db, "code": params.code},
            properties={"source_url": "https://www.stat-search.boj.or.jp"},
        ),
    )


@enumerator(output=BOJ_ENUMERATE_OUTPUT, tags=["macro", "jp"])
async def enumerate_boj(params: BojEnumerateParams) -> pd.DataFrame:
    """Enumerate BoJ statistics by querying metadata for each canonical DB.

    Pipeline:

    1. Iterate ``_BOJ_DATABASES`` (49 entries from the official API
       manual).
    2. For each DB, ``GET /getMetadata?db={code}&lang=en`` with a
       browser User-Agent (Akamai blocks the default httpx UA).
    3. Concurrency is capped at 2 with a 0.5s inter-request delay; 403/
       429/5xx responses retry up to 3 times with exponential backoff and
       honor ``Retry-After``. After exhausting retries we WARN and emit
       no rows for that DB.
    4. Emit one row per series (``code=SERIES_CODE``,
       ``entity_type='series'``) and one DB-level row
       (``code='db:{code}'``, ``entity_type='db'``).

    Layer header rows in BoJ metadata (``SERIES_CODE`` empty) are not
    emitted as catalog rows; they're consumed to build a breadcrumb path
    that's stamped onto every subsequent series row in the same DB.
    """
    semaphore = asyncio.Semaphore(_METADATA_CONCURRENCY)
    headers = {"User-Agent": _BROWSER_USER_AGENT}

    rows: list[dict[str, str]] = []
    failed_dbs: list[str] = []

    async with httpx.AsyncClient(timeout=60.0, headers=headers) as client:
        # asyncio.gather preserves order, so the published catalog is
        # deterministic across runs even when individual DBs interleave
        # under the concurrency cap.
        tasks = [
            _fetch_metadata(client, db_code, semaphore)
            for db_code, _category, _title in _BOJ_DATABASES
        ]
        payloads = await asyncio.gather(*tasks)

    for (db_code, db_category, db_title), payload in zip(_BOJ_DATABASES, payloads, strict=True):
        if payload is None:
            failed_dbs.append(db_code)
            continue
        rows.extend(
            _emit_rows_for_db(
                db_code=db_code,
                db_title=db_title,
                db_category=db_category,
                metadata=payload,
            )
        )

    if failed_dbs:
        logger.info(
            "BoJ enumerate: %d/%d DBs failed metadata fetch: %s",
            len(failed_dbs),
            len(_BOJ_DATABASES),
            ", ".join(failed_dbs),
        )
    else:
        logger.info("BoJ enumerate: all %d DBs fetched successfully", len(_BOJ_DATABASES))

    columns = list(_ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_boj.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    BOJ_SEARCH_OUTPUT,
    PARSIMONY_BOJ_CATALOG_URL_ENV,
    BojSearchParams,
    boj_search,
)

CATALOGS: list[tuple[str, object]] = [("boj", enumerate_boj)]

CONNECTORS = Connectors([boj_fetch, enumerate_boj, boj_search])
