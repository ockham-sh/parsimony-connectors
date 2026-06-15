"""BdE catalog enumeration connector.

Discovers BdE statistical series from the published catalog. Six of the seven
chapters are crawled as ``catalogo_*.csv`` files; the seventh, the Bank Lending
Survey (``pb``), is recovered from the bulk ``pb.zip`` because its CSV lists
un-fetchable family aliases rather than real series codes (see ``_http`` and
``_catalog.parse_pb_zip``). The crawl uses the shared ``ThrottledJsonFetcher``
(throttled, retrying serial fan-out over a raw ``httpx.Client``).

Best-effort by design: a per-source failure is logged and skipped so a partial
catalog is still produced (catalog publish jobs check ``len(df) == 0``
separately). The combined rows are de-duplicated by ``key`` — a series can be
listed under more than one thematic chapter (≈24% of raw rows are such
cross-chapter repeats), and the first occurrence in chapter order wins so the
result is deterministic. The returned frame matches ``ENUMERATE_COLUMNS``
exactly, as the ``@enumerator`` contract requires (it drops unmapped columns
then demands an exact match against the declared schema).
"""

from __future__ import annotations

import logging

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import MetadataCrawlConfig, ThrottledJsonFetcher

from parsimony_bde._http import (
    CATALOG_CHAPTERS,
    CATALOG_CSV_BASE_URL,
    CSV_ENCODING,
    PB_ZIP_URL,
)
from parsimony_bde.connectors._catalog import parse_catalog_csv, parse_pb_zip
from parsimony_bde.outputs import BDE_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)

METADATA_CRAWL = MetadataCrawlConfig(inter_request_delay_s=0.25)


def _fetch_catalog_chapter(
    fetcher: ThrottledJsonFetcher,
    chapter: str,
    category: str,
) -> list[dict[str, str]]:
    """Fetch + decode + parse one ``catalogo_*.csv`` chapter into enumerator rows."""
    url = f"{CATALOG_CSV_BASE_URL}/catalogo_{chapter}.csv"
    raw_bytes = fetcher.get_content(url, label=f"catalogo_{chapter}")
    if raw_bytes is None:
        logger.warning("BdE catalog chapter %r unavailable after retries", chapter)
        return []
    try:
        text = raw_bytes.decode(CSV_ENCODING)
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1", errors="replace")
    return parse_catalog_csv(text, category=category)


def _fetch_pb_survey(fetcher: ThrottledJsonFetcher) -> list[dict[str, str]]:
    """Fetch + parse the Bank Lending Survey bulk ``pb.zip`` into enumerator rows."""
    raw_bytes = fetcher.get_content(PB_ZIP_URL, label="pb_zip")
    if raw_bytes is None:
        logger.warning("BdE Bank Lending Survey (pb.zip) unavailable after retries")
        return []
    return parse_pb_zip(raw_bytes)


def _dedupe_by_key(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Keep the first row per ``key`` (cross-chapter repeats collapse here)."""
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for row in rows:
        key = row["key"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


@enumerator(output=BDE_ENUMERATE_OUTPUT, tags=["macro", "es"])
def enumerate_bde() -> pd.DataFrame:
    """Enumerate BdE statistical series from the published catalog (6 CSV chapters
    + the Bank Lending Survey recovered from ``pb.zip``), de-duplicated by code.

    Titles and descriptions are in Spanish — BdE's catalog CSV chapters have no
    English variant.
    """
    rows: list[dict[str, str]] = []
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        fetcher = ThrottledJsonFetcher(
            client,
            provider="bde",
            config=METADATA_CRAWL,
            logger=logger,
        )

        def _one(chapter: str, category: str) -> list[dict[str, str]]:
            return _fetch_catalog_chapter(fetcher, chapter, category)

        # Order matters: CSV chapters come before pb.zip so that on a
        # cross-source duplicate the CSV chapter wins in _dedupe_by_key (first
        # occurrence). Process serially to preserve that order.
        for chapter, category in CATALOG_CHAPTERS:
            rows.extend(_one(chapter, category))
        rows.extend(_fetch_pb_survey(fetcher))

    # @enumerator drops unmapped columns then requires an EXACT match — build
    # the frame with exactly the declared columns (header-only if all failed).
    return pd.DataFrame(_dedupe_by_key(rows), columns=list(ENUMERATE_COLUMNS))
