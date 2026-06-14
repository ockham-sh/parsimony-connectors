"""Build-time catalog enrichment: rich bilingual metadata from ``/series/``.

The hierarchy crawl (``enumerate_bdp``) only discovers series ids + a terse
English ``label``. The ``/series/?series_ids=`` endpoint returns a far richer
``description`` (full topic breadcrumb + transformation + unit, e.g. *"… -
consolidated data - transactions for the year ending the quarter in millions of
euros"*) in either language. The catalog build calls this once per language
(EN primary, PT for Portuguese recall on the BM25 index) and overlays the result
onto the enumerated rows via ``connectors._catalog.apply_enrichment``.

``/series/`` carries no observations, so this is cheap relative to the crawl:
~721 requests per language (100 ids/call). Best-effort, batched, retried, and
split-on-failure — a stubborn batch is halved rather than silently dropping 100
series' descriptions (the failure mode observed on the BdE enrichment).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from parsimony_bdp._http import BASE_URL, HEADERS, SERIES_BATCH

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 4
_MIN_SPLIT = 10  # stop splitting a stubborn batch below this size


def _parse_series(data: Any) -> dict[str, dict[str, str]]:
    """Extract ``{series_id: {label, short_label, description}}`` from a response list."""
    out: dict[str, dict[str, str]] = {}
    if not isinstance(data, list):
        return out
    for item in data:
        if not isinstance(item, dict):
            continue
        sid = item.get("id")
        if sid is None:
            continue
        out[str(sid)] = {
            "label": str(item.get("label") or "").strip(),
            "short_label": str(item.get("short_label") or "").strip(),
            "description": str(item.get("description") or "").strip(),
        }
    return out


async def fetch_series_metadata(
    series_ids: list[str],
    *,
    lang: str,
    concurrency: int = 4,
) -> dict[str, dict[str, str]]:
    """Map ``series_id → {label, short_label, description}`` via ``/series/``.

    ``lang`` is ``"EN"`` or ``"PT"``. Batched at 100 ids/call (the endpoint cap),
    retried with backoff, and split-on-failure down to a floor of 10. Best-effort
    overall: a sub-batch that still fails below the floor is logged and skipped,
    and those series simply miss this language's enrichment.
    """
    if not series_ids:
        return {}
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(timeout=60.0, headers=HEADERS, follow_redirects=True) as client:

        async def _attempt(batch: list[str]) -> Any | None:
            for attempt in range(_MAX_ATTEMPTS):
                try:
                    async with sem:
                        resp = await client.get(
                            f"{BASE_URL}/series/",
                            params={"series_ids": ",".join(batch), "lang": lang},
                        )
                    resp.raise_for_status()
                    return resp.json()
                except (httpx.HTTPError, ValueError) as exc:
                    if attempt == _MAX_ATTEMPTS - 1:
                        logger.debug("BdP /series/ batch (%d ids, %s) failed: %s", len(batch), lang, exc)
                        return None
                    await asyncio.sleep(0.5 * (attempt + 1))
            return None

        async def _resolve(batch: list[str]) -> dict[str, dict[str, str]]:
            data = await _attempt(batch)
            if data is not None:
                return _parse_series(data)
            if len(batch) > _MIN_SPLIT:
                mid = len(batch) // 2
                left, right = await asyncio.gather(_resolve(batch[:mid]), _resolve(batch[mid:]))
                return {**left, **right}
            logger.warning("BdP /series/ enrichment dropped %d ids (%s) after retries", len(batch), lang)
            return {}

        batches = [series_ids[i : i + SERIES_BATCH] for i in range(0, len(series_ids), SERIES_BATCH)]
        results = await asyncio.gather(*[_resolve(b) for b in batches])

    merged: dict[str, dict[str, str]] = {}
    for partial in results:
        merged.update(partial)
    return merged


__all__ = ["fetch_series_metadata"]
