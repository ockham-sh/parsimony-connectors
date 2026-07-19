"""BdF catalog enumeration connector (archetype A: live full-index export).

The Webstat ``series`` dataset is a single flat queryable table holding the
*entire* BdF universe (~41.6k series), so one ``/series/exports/json`` call with
a lean column projection streams every addressable unit — no per-dataset
fan-out. A second call to ``webstat-datasets`` supplies the 45 dataflow stubs
and the dataset names used as series context. Two requests, fully self-tracking:
completeness is verifiable by diffing ``len(catalog)`` against the live
``series`` ``total_count``.

The two universe sources are exposed as module-level seams (:func:`_list_datasets`,
:func:`_list_all_series`) so tests can monkeypatch them to a tiny slice and bound
the crawl — never pulling the full table offline.

Best-effort: a failed source is logged and skipped so a partial catalog still
builds; the publish job checks ``len(df) == 0`` separately. The returned frame
matches ``ENUMERATE_COLUMNS`` exactly, as the ``@enumerator`` contract requires.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import ThrottledJsonFetcher

from parsimony_bdf._http import (
    BASE_URL,
    CRAWL_TIMEOUT,
    DATASETS_PATH,
    DATASETS_SELECT,
    METADATA_CRAWL,
    SERIES_PATH,
    SERIES_SELECT,
    auth_headers,
    resolve_key,
)
from parsimony_bdf.connectors._catalog import build_enumerate_rows
from parsimony_bdf.outputs import BDF_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)


def _list_datasets(fetcher: ThrottledJsonFetcher) -> list[dict[str, Any]]:
    """Return every BdF dataflow stub (45 rows) from the ``webstat-datasets`` export.

    A crawl seam: tests monkeypatch this to a small slice to bound the fan-out.
    """
    url = f"{BASE_URL}/{DATASETS_PATH}"
    payload = fetcher.get_json(url, params={"select": DATASETS_SELECT, "order_by": "dataset_id"}, label="datasets")
    if not isinstance(payload, list):
        return []
    return [d for d in payload if isinstance(d, dict)]


def _list_all_series(fetcher: ThrottledJsonFetcher) -> list[dict[str, Any]] | None:
    """Return every series row (~41.6k) from the flat ``series`` export in one call.

    A crawl seam: the live integration test monkeypatches this to a single small
    dataset so it verifies the real export shape without streaming the whole
    table. Returns ``None`` on transport / parse failure so the caller can still
    emit the dataset stubs.
    """
    url = f"{BASE_URL}/{SERIES_PATH}"
    payload = fetcher.get_json(url, params={"select": SERIES_SELECT}, label="series")
    if payload is None:
        return None
    if not isinstance(payload, list):
        return []
    return [s for s in payload if isinstance(s, dict)]


@enumerator(output=BDF_ENUMERATE_OUTPUT, tags=["macro", "fr"], secrets=("api_key",))
def enumerate_bdf(*, api_key: str = "") -> pd.DataFrame:
    """Enumerate every Banque de France series with parent-dataset context.

    Streams the full ``series`` table plus the 45 dataflow stubs (two requests),
    emitting one row per series (KEY = ``series_key``) and one ``dataset:{id}``
    stub per dataflow, with bilingual descriptions and breadcrumb paths.
    """
    key = resolve_key(api_key)

    datasets: list[dict[str, Any]] = []
    series: list[dict[str, Any]] | None = []

    with httpx.Client(timeout=CRAWL_TIMEOUT, headers=auth_headers(key), follow_redirects=True) as client:
        fetcher = ThrottledJsonFetcher(client, provider="bdf", config=METADATA_CRAWL, logger=logger)
        datasets = _list_datasets(fetcher)
        series = _list_all_series(fetcher)

    if not datasets:
        logger.warning("BdF enumerate: dataset list unavailable; emitting series only")
    if series is None:
        logger.warning("BdF enumerate: series export failed; emitting dataset stubs only")
        series = []

    rows = build_enumerate_rows(datasets, series)
    n_series = sum(1 for r in rows if r["entity_type"] == "series")
    logger.info(
        "BdF enumerate: %d datasets, %d series, %d total catalog rows",
        len(datasets),
        n_series,
        len(rows),
    )

    return pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))


__all__ = ["enumerate_bdf"]
