"""World Bank indicator search connector.

Search the World Bank indicator catalogue by keyword. Uses the per-source
indicator endpoint (``/v2/source/{id}/indicator``) because the top-level
``/v2/indicator?search=`` parameter is silently **ignored** by the API
(it returns all ~29 000 indicators unfiltered).

Source 2 (World Development Indicators) has ~1 500 indicators fetched in
~15 pages at 100 per page — a 1–2 second operation. Filtering is done
**client-side** by a case-insensitive substring match on ``name``.
"""

# GREP_SUMMARY: worldbank_search — per-source indicator search with client-side name filter

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError
from parsimony.transport.helpers import fetch_json, make_http_client

from parsimony_worldbank._http import BASE_URL, DEFAULT_PAGE_SIZE, HEADERS, MAX_PAGES
from parsimony_worldbank.outputs import SEARCH_COLUMNS, WORLDBANK_SEARCH_OUTPUT

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_indicator_record(record: dict[str, Any]) -> dict[str, Any]:
    """Flatten a raw World Bank indicator record into search columns.

    The API returns nested ``source`` (``{id, value}``) and ``topics``
    (``[{id, value}, …]``) objects; we convert them to scalar strings.

    @startcontract
    @brief Flatten indicator API response to search row.
    @param record: Raw dict from the World Bank indicator endpoint.
    @return Dict with keys matching SEARCH_COLUMNS.
    @invariant All returned values are strings (empty string for missing).
    @endcontract
    """
    source = record.get("source") or {}
    topics_raw = record.get("topics") or []
    topic_ids = ",".join(
        str(t.get("id", "")) for t in topics_raw if isinstance(t, dict)
    )
    return {
        "indicator_id": str(record.get("id", "")),
        "indicator_name": str(record.get("name", "")),
        "source_id": str(source.get("id", "")),
        "source_name": str(source.get("value", "")),
        "topic_ids": topic_ids,
    }


def _name_contains(row: dict[str, Any], query_lower: str) -> bool:
    """Return ``True`` if *query_lower* is a substring of ``indicator_name``."""
    return query_lower in row["indicator_name"].lower()


def _fetch_indicator_pages(
    client: Any,
    path: str,
    params: dict[str, Any],
    provider: str,
    op_prefix: str,
) -> list[dict[str, Any]]:
    """Paginate through a World Bank indicator endpoint.

    Iterates over pages (up to ``MAX_PAGES``), collects all records, and
    stops when the response metadata indicates there are no more pages.

    @startcontract
    @brief Fetch all pages from a paginated World Bank endpoint.
    @param client: HTTP client from ``make_http_client``.
    @param path: API path (e.g. ``/source/2/indicator``).
    @param params: Query parameters (``page`` is set per-iteration).
    @param provider: Provider name for error reporting.
    @param op_prefix: Operation name prefix for observability.
    @return List of raw record dicts (possibly empty).
    @invariant Never returns ``None`` — always a list.
    @endcontract
    """
    # region FUNC_fetch_indicator_pages
    all_records: list[dict[str, Any]] = []
    page = 1

    while page <= MAX_PAGES:
        params["page"] = page
        data = fetch_json(
            client,
            path=path,
            params=params,
            provider=provider,
            op_name=f"{op_prefix}_{page}",
        )

        if not isinstance(data, list) or len(data) < 2:
            break

        records = data[1]
        if not isinstance(records, list) or not records:
            break

        for record in records:
            if isinstance(record, dict):
                all_records.append(record)

        # Check termination condition from response metadata
        meta = data[0]
        if isinstance(meta, dict):
            total = meta.get("total", 0)
            per = meta.get("per_page", params.get("per_page", DEFAULT_PAGE_SIZE))
            if isinstance(total, (int, float)) and isinstance(per, (int, float)):
                total_pages = -(-int(total) // int(per))  # ceil division
                if page >= total_pages:
                    break

        page += 1

    return all_records
    # endregion


def _fetch_sources(client: Any) -> list[dict[str, Any]]:
    """Fetch all World Bank data sources (paginated).

    Returns a list of source dicts with ``id`` and ``name`` keys.
    """
    # region FUNC_fetch_sources
    all_sources: list[dict[str, Any]] = []
    page = 1

    while page <= MAX_PAGES:
        data = fetch_json(
            client,
            path="/source",
            params={"format": "json", "per_page": DEFAULT_PAGE_SIZE, "page": page},
            provider="worldbank",
            op_name=f"list_sources_{page}",
        )

        if not isinstance(data, list) or len(data) < 2:
            break

        records = data[1]
        if not isinstance(records, list) or not records:
            break

        for record in records:
            if isinstance(record, dict):
                all_sources.append(record)

        meta = data[0]
        if isinstance(meta, dict):
            total = meta.get("total", 0)
            per = meta.get("per_page", DEFAULT_PAGE_SIZE)
            if isinstance(total, (int, float)) and isinstance(per, (int, float)):
                total_pages = -(-int(total) // int(per))
                if page >= total_pages:
                    break

        page += 1

    return all_sources
    # endregion


# ---------------------------------------------------------------------------
# Public connector
# ---------------------------------------------------------------------------


@connector(output=WORLDBANK_SEARCH_OUTPUT, tags=["macro", "international", "development"])
def worldbank_search(
    query: Annotated[str, "Search keyword(s) — matched against indicator name (case-insensitive)"],
    source_id: Annotated[
        int,
        "World Bank source database ID (default 2 = World Development Indicators). "
        "Pass 0 or negative to search ALL sources (slower — fetches every source's catalogue).",
    ] = 2,
    topic_id: Annotated[int | None, "Optional topic ID to narrow results within a topic"] = None,
) -> pd.DataFrame:
    """Search World Bank indicator catalogue by keyword.

    Uses ``/v2/source/{id}/indicator`` because ``/v2/indicator?search=`` is
    silently ignored. Source 2 has ~1 500 indicators (~15 pages at 100/page).

    Parameters
    ----------
    query:
        Free-text search string matched case-insensitively against ``name``.
    source_id:
        Source database ID (default 2 = World Development Indicators).
        Pass 0 or negative to scan ALL sources (slower).
    topic_id:
        Optional topic ID. Uses ``/v2/topic/{id}/indicator`` when given.

    Returns
    -------
    pd.DataFrame
        One row per match: ``indicator_id``, ``indicator_name``,
        ``source_id``, ``source_name``, ``topic_ids``.
    """
    # region FUNC_worldbank_search

    # --- Validate query -------------------------------------------------------
    query = query.strip()
    if not query:
        raise EmptyDataError(
            "worldbank",
            message="Search query must be non-empty",
        )

    query_lower = query.lower()
    client = make_http_client(BASE_URL, headers=HEADERS, timeout=30.0)
    raw_records: list[dict[str, Any]] = []

    # --- Fetch raw records ----------------------------------------------------
    if topic_id is not None:
        # [BELIEF: topic_id narrows the search scope server-side]
        # | [INPUT: topic_id={topic_id}]
        # | [EXPECTING: all indicators belonging to that topic]
        logger.info("Searching indicators for topic_id=%s", topic_id)
        path = f"/topic/{topic_id}/indicator"
        params: dict[str, Any] = {
            "format": "json",
            "per_page": DEFAULT_PAGE_SIZE,
        }
        raw_records = _fetch_indicator_pages(
            client,
            path=path,
            params=params,
            provider="worldbank",
            op_prefix=f"search_topic_{topic_id}",
        )
    elif source_id > 0:
        # [BELIEF: source_id scopes to a single database]
        # | [INPUT: source_id={source_id}]
        # | [EXPECTING: ~1500 indicators for source 2, fetched in ~15 pages]
        path = f"/source/{source_id}/indicator"
        params = {
            "format": "json",
            "per_page": DEFAULT_PAGE_SIZE,
        }
        raw_records = _fetch_indicator_pages(
            client,
            path=path,
            params=params,
            provider="worldbank",
            op_prefix=f"search_source_{source_id}",
        )
    else:
        # source_id <= 0 — scan all sources
        # [BELIEF: scanning all sources is correct when source_id <= 0]
        # | [INPUT: source_id={source_id}]
        # | [EXPECTING: all indicator records aggregated across all sources]
        logger.warning(
            "Scanning ALL World Bank sources (source_id=%s) — this may be slow.",
            source_id,
        )
        sources = _fetch_sources(client)
        for src in sources:
            sid = src.get("id")
            if not sid:
                continue
            src_path = f"/source/{sid}/indicator"
            src_params: dict[str, Any] = {
                "format": "json",
                "per_page": DEFAULT_PAGE_SIZE,
            }
            src_records = _fetch_indicator_pages(
                client,
                path=src_path,
                params=src_params,
                provider="worldbank",
                op_prefix=f"search_all_source_{sid}",
            )
            raw_records.extend(src_records)

    # --- Early exit if no data returned ---------------------------------------
    if not raw_records:
        raise EmptyDataError(
            "worldbank",
            message=f"No indicators match query={query!r}",
            query_params={"query": query, "source_id": source_id, "topic_id": topic_id},
        )

    # --- Parse and filter -----------------------------------------------------
    parsed = [_parse_indicator_record(r) for r in raw_records]
    matched = [row for row in parsed if _name_contains(row, query_lower)]

    if not matched:
        raise EmptyDataError(
            "worldbank",
            message=f"No indicators match query={query!r}",
            query_params={"query": query, "source_id": source_id, "topic_id": topic_id},
        )

    return pd.DataFrame(matched, columns=list(SEARCH_COLUMNS))
    # endregion
