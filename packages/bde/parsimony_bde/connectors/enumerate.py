"""BdE catalog enumeration connector.

Discovers BdE statistical series by crawling the seven published catalog CSV
chapters concurrently. The crawl uses the shared ``ThrottledJsonFetcher``
(throttled, retrying fan-out over a raw ``httpx.Client``) — the re-base of
``_shared`` onto core transport is a separate cross-cutting step, so this code
keeps using ``_shared`` for now and only owns the BdE-side parsing + framing.

Best-effort by design: a per-chapter failure is logged and skipped so a partial
catalog is still produced (catalog publish jobs check ``len(df) == 0``
separately). The returned frame matches ``ENUMERATE_COLUMNS`` exactly, as the
``@enumerator`` contract requires (it drops unmapped columns then demands an
exact match against the declared schema).
"""

from __future__ import annotations

import logging

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import MetadataCrawlConfig, ThrottledJsonFetcher

from parsimony_bde._http import CATALOG_CHAPTERS, CATALOG_CSV_BASE_URL, CSV_ENCODING
from parsimony_bde.connectors._catalog import parse_catalog_csv
from parsimony_bde.outputs import BDE_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)

METADATA_CRAWL = MetadataCrawlConfig(concurrency=4, inter_request_delay_s=0.25)


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


@enumerator(output=BDE_ENUMERATE_OUTPUT, tags=["macro", "es"])
def enumerate_bde() -> pd.DataFrame:
    """Enumerate BdE statistical series from the published catalog CSV chapters."""
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

        chapter_results = [_one(chapter, category) for chapter, category in CATALOG_CHAPTERS]
        for chapter_rows in chapter_results:
            rows.extend(chapter_rows)

    # @enumerator drops unmapped columns then requires an EXACT match — build
    # the frame with exactly the declared columns (header-only if all failed).
    return pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))
