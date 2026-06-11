"""Bank of Japan (BoJ): fetch + catalog enumeration.

API manual: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf
No authentication required (keyless public JSON API). Max 250 codes per
request.

``boj_fetch`` uses the canonical ``make_http_client`` + ``fetch_json`` transport
(GET + raise_for_status + map_http_error + map_timeout_error + JSON parse +
None-param drop in one call). The enumerator keeps the shared
``ThrottledJsonFetcher`` for the Akamai-aware metadata crawl — the re-base of
``_shared`` onto core transport is a separate cross-cutting step.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, cast

import httpx
import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
)
from parsimony.transport.helpers import fetch_json, make_http_client
from parsimony_shared.cb_enumerate import MetadataCrawlConfig, ThrottledJsonFetcher

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.stat-search.boj.or.jp/api/v1"
_MAX_CODES = 250

# BoJ's stat_search endpoints sit behind Akamai, which can block both the
# default httpx User-Agent and high-concurrency fan-outs. Empirically a
# concurrency cap of 2, a small inter-request delay, and a browser UA are
# enough to keep the metadata crawl stable; higher concurrency triggers 403s.
# (The single-shot ``getDataCode`` data endpoint does not need the browser UA
# from every probed network, but we send it there too for symmetry / safety.)
_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_METADATA_CRAWL = MetadataCrawlConfig(
    concurrency=2,
    inter_request_delay_s=0.5,
    retry_statuses=frozenset({403, 429, 500, 502, 503, 504}),
)

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
        # ``description`` carries searchable prose: breadcrumb + category +
        # unit + frequency + parent DB title (and NOTES if present) for
        # series; a short summary for DB rows.
        Column(name="description", role=ColumnRole.METADATA),
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
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


_ENUMERATE_COLUMNS: tuple[str, ...] = tuple(c.name for c in BOJ_ENUMERATE_OUTPUT.columns)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_boj_date(date_str: str, freq: str) -> str:
    """Parse a BoJ survey-date token into an ISO ``YYYY-MM-DD`` string.

    BoJ returns survey dates as compact integers/strings whose width depends
    on the series frequency (``19990101`` daily, ``199901`` monthly, ``1999``
    annual, ``199901`` for quarter-of-year). Unrecognised widths pass through
    unchanged so the downstream ``dtype="datetime"`` coercion can surface a
    real parse problem rather than us silently mangling the value.
    """
    freq_lower = freq.lower()
    if freq_lower in ("dm", "daily", "weekly"):
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
    elif freq_lower in ("am", "annual", "sm", "semi-annual", "semiannual") and len(date_str) >= 4:
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


def _validate_codes(code: str) -> str:
    """Validate + normalise the comma-separated ``code`` argument.

    Returns the cleaned comma-joined string. Raises ``InvalidParameterError``
    for an empty list or more than ``_MAX_CODES`` codes.
    """
    codes = [s.strip() for s in code.split(",") if s.strip()]
    if not codes:
        raise InvalidParameterError("boj", "At least one series code required")
    if len(codes) > _MAX_CODES:
        raise InvalidParameterError("boj", f"Maximum {_MAX_CODES} codes per request")
    return ",".join(codes)


def _fetch_metadata(fetcher: ThrottledJsonFetcher, db: str) -> dict[str, Any] | None:
    """Fetch one DB's metadata via the shared throttled fetcher."""
    return cast(
        dict[str, Any] | None,
        fetcher.get_json(
            f"{_BASE_URL}/getMetadata",
            params={"db": db, "lang": "en"},
            label=db,
        ),
    )


def _is_header_row(series_row: dict[str, Any]) -> bool:
    """A metadata row with no ``SERIES_CODE`` is a section header.

    Section headers carry the section title in ``NAME_OF_TIME_SERIES`` and a
    section ordinal in ``LAYER1`` (with ``LAYER2..5 == 0``); series rows carry
    a real ``SERIES_CODE`` and their parent-section ordinal in ``LAYER1``.
    """
    return not (series_row.get("SERIES_CODE") or "").strip()


def _layer1_ordinal(series_row: dict[str, Any]) -> int | None:
    """Return the ``LAYER1`` section ordinal as an int, or ``None``.

    BoJ encodes ``LAYER1`` as a JSON integer (e.g. ``1``, ``2``). It is a
    POSITION index into the DB's section list, NOT a title — the section title
    lives in the matching header row's ``NAME_OF_TIME_SERIES``.
    """
    raw = series_row.get("LAYER1")
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


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

    Always emits a DB-level row. Series rows come from ``RESULTSET`` entries
    that carry a non-empty ``SERIES_CODE``. Section header rows (empty
    ``SERIES_CODE``) define the breadcrumb: each header's
    ``NAME_OF_TIME_SERIES`` is the section title for its ``LAYER1`` ordinal,
    and every series inherits the title of the most-recent header at its own
    ``LAYER1`` ordinal.
    """
    rows: list[dict[str, str]] = []
    # ``LAYER1`` ordinal -> section title, populated as header rows stream by.
    section_titles: dict[int, str] = {}
    n_series = 0
    top_sections: list[str] = []
    seen_top: set[str] = set()

    result_set = (metadata or {}).get("RESULTSET") or []
    if not isinstance(result_set, list):
        result_set = []

    for series in result_set:
        if not isinstance(series, dict):
            continue

        ordinal = _layer1_ordinal(series)

        if _is_header_row(series):
            # Section header: record its title for the LAYER1 ordinal so the
            # following series rows can build a breadcrumb. Track top-level
            # section titles for the DB description.
            section_title = str(series.get("NAME_OF_TIME_SERIES") or "").strip()
            if ordinal is not None and section_title:
                section_titles[ordinal] = section_title
                if section_title not in seen_top:
                    seen_top.add(section_title)
                    top_sections.append(section_title)
            continue

        series_code = (series.get("SERIES_CODE") or "").strip()
        breadcrumb = section_titles.get(ordinal, "") if ordinal is not None else ""

        title = series.get("NAME_OF_TIME_SERIES") or series.get("NAME_OF_TIME_SERIES_J") or series_code
        unit = (series.get("UNIT") or "").strip()
        frequency = _normalize_frequency(series.get("FREQUENCY") or "")
        category = (series.get("CATEGORY") or db_category).strip()
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
def boj_fetch(
    db: str,
    code: Annotated[str, Namespace("boj")],
    start_date: str | None = None,
    end_date: str | None = None,
    lang: str = "en",
) -> pd.DataFrame:
    """Fetch Bank of Japan time series by database and series code(s).

    ``db`` is a BoJ statistics database code (e.g. ``FM08`` for FX rates,
    ``PR01`` for prices); ``code`` is one or more comma-separated series codes
    (max 250, e.g. ``FXERD01`` or ``FXERD01,FXERD04``). ``start_date`` /
    ``end_date`` are period strings whose format follows the series frequency
    (e.g. ``YYYYMM`` for monthly/daily-by-month). Returns one row per
    observation with ``code``, ``title``, ``date``, ``value``.
    """
    db_clean = db.strip().upper()
    if not db_clean:
        raise InvalidParameterError("boj", "db must be non-empty")
    codes = _validate_codes(code)

    req_params: dict[str, Any] = {
        "db": db_clean,
        "code": codes,
        "lang": lang,
        "startDate": start_date or None,
        "endDate": end_date or None,
    }
    body = fetch_json(
        make_http_client(_BASE_URL, headers={"User-Agent": _BROWSER_USER_AGENT}, timeout=60.0),
        path="getDataCode",
        params=req_params,
        provider="boj",
        op_name="series",
    )

    if not isinstance(body, dict):
        raise ParseError("boj", f"unexpected response shape for db={db_clean}, code={codes}")

    result_set = body.get("RESULTSET")
    if not result_set:
        raise EmptyDataError(
            "boj",
            message=f"No data returned for db={db_clean}, code={codes}",
            query_params={"db": db_clean, "code": codes},
        )
    if not isinstance(result_set, list):
        raise ParseError("boj", f"RESULTSET is not a list for db={db_clean}, code={codes}")

    rows: list[dict[str, Any]] = []
    for series in result_set:
        if not isinstance(series, dict):
            continue
        series_code = series.get("SERIES_CODE", "")
        name = series.get("NAME_OF_TIME_SERIES", series.get("NAME_OF_TIME_SERIES_J", series_code))
        freq = (series.get("FREQUENCY") or "").lower()
        values_block = series.get("VALUES") or {}
        dates = values_block.get("SURVEY_DATES", []) if isinstance(values_block, dict) else []
        values = values_block.get("VALUES", []) if isinstance(values_block, dict) else []

        if isinstance(dates, (str, int)):
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
                    "code": series_code,
                    "title": name,
                    "date": _parse_boj_date(str(date_str), freq),
                    "value": value,
                }
            )

    if not rows:
        raise EmptyDataError(
            "boj",
            message=f"No observations parsed for db={db_clean}, code={codes}",
            query_params={"db": db_clean, "code": codes},
        )

    return pd.DataFrame(rows)


def _resolve_boj_database(db_code: str) -> tuple[str, str, str]:
    normalized = db_code.strip().upper()
    for code, category, title in _BOJ_DATABASES:
        if code == normalized:
            return code, category, title
    raise InvalidParameterError("boj", f"Unknown BoJ database {db_code!r}")


def fetch_boj_enumeration_rows_for_db(db_code: str) -> pd.DataFrame:
    """Fetch catalog discovery rows for one BoJ database (no full 50-DB sweep)."""
    db_code, db_category, db_title = _resolve_boj_database(db_code)
    with httpx.Client(timeout=60.0, headers={"User-Agent": _BROWSER_USER_AGENT}) as client:
        fetcher = ThrottledJsonFetcher(client, provider="boj", config=_METADATA_CRAWL, logger=logger)
        payload = _fetch_metadata(fetcher, db_code)
    if payload is None:
        logger.warning("BoJ enumerate: metadata fetch failed for db=%s", db_code)
        return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))
    rows = _emit_rows_for_db(
        db_code=db_code,
        db_title=db_title,
        db_category=db_category,
        metadata=payload,
    )
    return pd.DataFrame(rows, columns=list(_ENUMERATE_COLUMNS))


@enumerator(output=BOJ_ENUMERATE_OUTPUT, tags=["macro", "jp"])
def enumerate_boj() -> pd.DataFrame:
    """Enumerate BoJ series by fetching metadata for each canonical database.

    Emits one series row per discovered series plus one synthetic ``db:<code>``
    row per database, with breadcrumb + coverage metadata for catalog
    discovery. The crawl is Akamai-throttled (bounded concurrency, browser UA,
    retries on 403/429/5xx); per-DB failures are logged and skipped so a
    partial catalog is still produced.
    """
    rows: list[dict[str, str]] = []
    failed_dbs: list[str] = []

    with httpx.Client(timeout=60.0, headers={"User-Agent": _BROWSER_USER_AGENT}) as client:
        fetcher = ThrottledJsonFetcher(client, provider="boj", config=_METADATA_CRAWL, logger=logger)
        # Parallel per-db builds preserve submission order, so the published catalog is
        # deterministic across runs even when individual DBs interleave
        # under the concurrency cap.
        tasks = [_fetch_metadata(fetcher, db_code) for db_code, _category, _title in _BOJ_DATABASES]
        payloads = tasks

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
        logger.warning(
            "BoJ enumerate: %d/%d DBs failed metadata fetch: %s",
            len(failed_dbs),
            len(_BOJ_DATABASES),
            ", ".join(failed_dbs),
        )
    else:
        logger.info("BoJ enumerate: all %d DBs fetched successfully", len(_BOJ_DATABASES))

    return pd.DataFrame(rows, columns=list(_ENUMERATE_COLUMNS))


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_boj.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    PARSIMONY_BOJ_CATALOG_URL_ENV,
    boj_databases_search,
    boj_series_search,
)

CONNECTORS = Connectors([boj_fetch, enumerate_boj, boj_databases_search, boj_series_search])
