"""``enumerate_boj`` — the BoJ catalog feed (archetype C + B).

Fans a ``getMetadata`` request across the 50-DB registry (the C part — the API
exposes no way to list databases) and, for each DB, parses the complete
``RESULTSET`` (the B part — ``getMetadata`` is uncapped, so one call per DB lists
*every* series). Emits one row per series plus one synthetic ``db:<code>`` row
per database. Per-DB failures are logged and skipped (best-effort) so a partial
catalog still builds; a ``failed/total`` summary is logged.

The fan-out processes each DB's payload serially and releases it before the
next, so a giant response (``CO``/TANKAN is ~99 MB) is not held alongside the
others.
"""

from __future__ import annotations

import logging
from typing import Any, cast

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import ThrottledJsonFetcher

from parsimony_boj._http import BASE_URL, BROWSER_USER_AGENT, FETCH_TIMEOUT, METADATA_CRAWL, PROVIDER
from parsimony_boj.connectors.fetch import _normalize_frequency
from parsimony_boj.databases import _BOJ_DATABASES, _resolve_boj_database
from parsimony_boj.outputs import BOJ_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)


def _list_databases() -> tuple[tuple[str, str, str], ...]:
    """Return the DB registry. A seam: live tests monkeypatch this to bound the
    fan-out to one or two databases."""
    return _BOJ_DATABASES


def _fetch_metadata(fetcher: ThrottledJsonFetcher, db: str) -> dict[str, Any] | None:
    """Fetch one DB's metadata via the shared throttled fetcher (``None`` on failure)."""
    return cast(
        "dict[str, Any] | None",
        fetcher.get_json(f"{BASE_URL}/getMetadata", params={"db": db, "lang": "en"}, label=db),
    )


def _is_header_row(series_row: dict[str, Any]) -> bool:
    """A metadata row with no ``SERIES_CODE`` is a section header.

    Section headers carry the section title in ``NAME_OF_TIME_SERIES`` and a
    section ordinal in ``LAYER1``; series rows carry a real ``SERIES_CODE`` and
    their parent-section ordinal in ``LAYER1``.
    """
    return not (series_row.get("SERIES_CODE") or "").strip()


def _layer1_ordinal(series_row: dict[str, Any]) -> int | None:
    """Return the ``LAYER1`` section ordinal as an int, or ``None``.

    ``LAYER1`` is a POSITION index into the DB's section list, NOT a title — the
    section title lives in the matching header row's ``NAME_OF_TIME_SERIES``.
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
    """Assemble the per-series DESCRIPTION text fed to the embedder/BM25 index."""
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


def _db_description(*, category: str, n_series: int, db_code: str, top_sections: list[str]) -> str:
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

    Always emits a DB-level row. Series rows come from ``RESULTSET`` entries with
    a non-empty ``SERIES_CODE``. Section header rows (empty ``SERIES_CODE``)
    define the breadcrumb: each header's ``NAME_OF_TIME_SERIES`` is the section
    title for its ``LAYER1`` ordinal, and every series inherits the title of the
    most-recent header at its own ``LAYER1`` ordinal.
    """
    rows: list[dict[str, str]] = []
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


def _enumerate_one(
    fetcher: ThrottledJsonFetcher,
    db_code: str,
    db_category: str,
    db_title: str,
) -> tuple[str, list[dict[str, str]] | None]:
    """Fetch + parse one DB, returning its rows (or ``None`` on fetch failure).

    Parsing happens here so the (potentially huge) raw payload is released as
    soon as the rows are extracted, instead of being held alongside the others.
    """
    payload = _fetch_metadata(fetcher, db_code)
    if payload is None:
        return db_code, None
    return db_code, _emit_rows_for_db(
        db_code=db_code,
        db_title=db_title,
        db_category=db_category,
        metadata=payload,
    )


def fetch_boj_enumeration_rows_for_db(db_code: str) -> pd.DataFrame:
    """Fetch catalog discovery rows for one BoJ database (no full 50-DB sweep)."""
    db_code, db_category, db_title = _resolve_boj_database(db_code)
    with httpx.Client(timeout=FETCH_TIMEOUT, headers={"User-Agent": BROWSER_USER_AGENT}) as client:
        fetcher = ThrottledJsonFetcher(client, provider=PROVIDER, config=METADATA_CRAWL, logger=logger)
        _, rows = _enumerate_one(fetcher, db_code, db_category, db_title)
    if rows is None:
        logger.warning("BoJ enumerate: metadata fetch failed for db=%s", db_code)
        return pd.DataFrame(columns=list(ENUMERATE_COLUMNS))
    return pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))


@enumerator(output=BOJ_ENUMERATE_OUTPUT, tags=["macro", "jp"])
def enumerate_boj() -> pd.DataFrame:
    """Enumerate BoJ series by fetching metadata for each registry database.

    Emits one series row per discovered series plus one synthetic ``db:<code>``
    row per database, with breadcrumb + coverage metadata for catalog discovery.
    The crawl is Akamai-throttled (browser UA, retries on 403/429/5xx); per-DB
    failures are logged and skipped so a partial catalog is still produced.
    """
    databases = _list_databases()
    rows: list[dict[str, str]] = []
    failed_dbs: list[str] = []

    with httpx.Client(timeout=FETCH_TIMEOUT, headers={"User-Agent": BROWSER_USER_AGENT}) as client:
        fetcher = ThrottledJsonFetcher(client, provider=PROVIDER, config=METADATA_CRAWL, logger=logger)
        # Serial fan-out: processes each DB in order so the published catalog is
        # deterministic across runs and giant payloads (CO/TANKAN ~99 MB) are
        # released immediately after parsing.
        results = [
            _enumerate_one(fetcher, db_code, db_category, db_title)
            for db_code, db_category, db_title in databases
        ]

    for db_code, db_rows in results:
        if db_rows is None:
            failed_dbs.append(db_code)
            continue
        rows.extend(db_rows)

    if failed_dbs:
        logger.warning(
            "BoJ enumerate: %d/%d DBs failed metadata fetch: %s",
            len(failed_dbs),
            len(databases),
            ", ".join(failed_dbs),
        )
    else:
        logger.info("BoJ enumerate: all %d DBs fetched successfully (%d rows)", len(databases), len(rows))

    return pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))


__all__ = [
    "_emit_rows_for_db",
    "_list_databases",
    "enumerate_boj",
    "fetch_boj_enumeration_rows_for_db",
]
