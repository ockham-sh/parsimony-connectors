"""BdE catalog enumeration connector."""

from __future__ import annotations

import asyncio
import logging

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import MetadataCrawlConfig, ThrottledJsonFetcher

from parsimony_bde._http import CATALOG_CSV_BASE_URL, CATALOG_CHAPTERS, CSV_ENCODING
from parsimony_bde.connectors._catalog import parse_catalog_csv
from parsimony_bde.outputs import BDE_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS
from parsimony_bde.params import BdeEnumerateParams

logger = logging.getLogger(__name__)

METADATA_CRAWL = MetadataCrawlConfig(concurrency=4, inter_request_delay_s=0.25)


async def _fetch_catalog_chapter(
    fetcher: ThrottledJsonFetcher,
    chapter: str,
    category: str,
) -> list[dict[str, str]]:
    url = f"{CATALOG_CSV_BASE_URL}/catalogo_{chapter}.csv"
    raw_bytes = await fetcher.get_content(url, label=f"catalogo_{chapter}")
    if raw_bytes is None:
        logger.warning("BdE catalog chapter %r unavailable after retries", chapter)
        return []
    try:
        text = raw_bytes.decode(CSV_ENCODING)
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1", errors="replace")
    return parse_catalog_csv(text, category=category)


@enumerator(output=BDE_ENUMERATE_OUTPUT, tags=["macro", "es"])
async def enumerate_bde() -> pd.DataFrame:
    """Enumerate BdE statistical series from published catalog CSV chapters."""
    BdeEnumerateParams()

    rows: list[dict[str, str]] = []
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        fetcher = ThrottledJsonFetcher(
            client,
            provider="bde",
            config=METADATA_CRAWL,
            logger=logger,
        )

        async def _one(chapter: str, category: str) -> list[dict[str, str]]:
            return await _fetch_catalog_chapter(fetcher, chapter, category)

        chapter_results = await asyncio.gather(
            *[_one(chapter, category) for chapter, category in CATALOG_CHAPTERS]
        )
        for chapter_rows in chapter_results:
            rows.extend(chapter_rows)

    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS)) if rows else pd.DataFrame(columns=list(ENUMERATE_COLUMNS))
    return df
