"""BdP catalog enumeration connector (archetype B — paginated hierarchy crawl).

Walks the BPstat hierarchy ``domains → datasets → series`` and emits one row per
addressable unit (synthetic ``domain:`` / ``dataset:`` navigation rows plus
``{domain}:{dataset}:{series}`` series rows). The crawl discovers ids + terse
English labels only; the rich bilingual descriptions are layered on at build
time (see ``catalog_build`` / ``enrich``).

Two things the previous connector got wrong, fixed here:

* **The datasets list is paginated.** ``/domains/{id}/datasets/`` caps at 10
  items/page by default; 3 domains have >10 datasets (domain 19 has 25), so
  reading only page 1 silently dropped datasets — and every series in them.
  ``_list_datasets`` now follows ``extension.next_page`` (with ``page_size=100``).
* **The dataset-detail crawl is lean.** ``page_size=100&obs_last_n=1`` pulls 100
  series/page with a one-point ``value`` array (~70 KB) instead of 10 series/page
  with full history (~7,200 pages, 502-prone). See ``_http.DATASET_CRAWL_PARAMS``.

Best-effort by design: a failed domain/dataset is logged and skipped so a partial
catalog still builds. Each dataset is self-checked — the crawled series count is
compared against the stub's declared ``num_series`` and any shortfall is logged.
The returned frame matches ``ENUMERATE_COLUMNS`` exactly (the ``@enumerator``
contract drops unmapped columns then demands an exact match).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import ThrottledJsonFetcher

from parsimony_bdp._http import (
    BASE_URL,
    DATASET_CRAWL_PARAMS,
    DATASET_PAGE_SIZE,
    HEADERS,
    MAX_PAGES_PER_DATASET,
    METADATA_CRAWL,
)
from parsimony_bdp.connectors import _catalog
from parsimony_bdp.outputs import BDP_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Network seams (module-level so tests can monkeypatch / bound the crawl)
# ---------------------------------------------------------------------------


def _list_domains(fetcher: ThrottledJsonFetcher) -> list[dict[str, Any]]:
    """Return the full BdP domain list (77 entries; empty on failure).

    The bounding seam for tests — monkeypatch this to a 1–2 domain slice so the
    crawl fires a handful of requests, never the full ~720-page fan-out.
    """
    payload = fetcher.get_json(f"{BASE_URL}/domains/", params={"lang": "EN"})
    if not isinstance(payload, list):
        return []
    return [d for d in payload if isinstance(d, dict)]


def _list_datasets(fetcher: ThrottledJsonFetcher, domain_id: int | str) -> list[dict[str, Any]]:
    """Return ALL dataset stubs under ``domain_id``, following pagination.

    ``/domains/{id}/datasets/`` paginates (default 10/page); we request
    ``page_size=100`` and follow ``extension.next_page`` so domains with >10
    datasets are fully enumerated.
    """
    stubs: list[dict[str, Any]] = []
    url: str | None = f"{BASE_URL}/domains/{domain_id}/datasets/"
    params: dict[str, Any] | None = {"lang": "EN", "page_size": DATASET_PAGE_SIZE}
    pages = 0
    while url and pages < MAX_PAGES_PER_DATASET:
        payload = fetcher.get_json(url, params=params)
        if not isinstance(payload, dict):
            break
        link = payload.get("link")
        items = link.get("item", []) if isinstance(link, dict) else []
        if isinstance(items, list):
            stubs.extend(it for it in items if isinstance(it, dict))
        ext = payload.get("extension")
        nxt = ext.get("next_page") if isinstance(ext, dict) else None
        url, params = (nxt, None) if isinstance(nxt, str) and nxt else (None, None)
        pages += 1
    return stubs


def _crawl_dataset_series(
    fetcher: ThrottledJsonFetcher,
    domain_id: int | str,
    dataset_id: str,
) -> list[dict[str, Any]] | None:
    """Walk the paginated dataset detail and collect every series stub.

    Returns the list of ``{id, label}`` stubs, or ``None`` if the very first
    page failed (so the caller can record the dataset as failed rather than
    empty). Deduped by id; follows ``extension.next_page``.
    """
    url: str | None = f"{BASE_URL}/domains/{domain_id}/datasets/{dataset_id}/"
    params: dict[str, Any] | None = {"lang": "EN", **DATASET_CRAWL_PARAMS}
    series: list[dict[str, Any]] = []
    seen: set[str] = set()
    pages = 0
    first = True
    while url and pages < MAX_PAGES_PER_DATASET:
        payload = fetcher.get_json(url, params=params)
        if not isinstance(payload, dict):
            if first:
                return None
            break
        first = False
        ext = payload.get("extension")
        items = ext.get("series", []) if isinstance(ext, dict) else []
        if isinstance(items, list):
            for s in items:
                if not isinstance(s, dict):
                    continue
                sid = str(s.get("id") or "").strip()
                if sid and sid not in seen:
                    seen.add(sid)
                    series.append(s)
        nxt = ext.get("next_page") if isinstance(ext, dict) else None
        url, params = (nxt, None) if isinstance(nxt, str) and nxt else (None, None)
        pages += 1
    if pages >= MAX_PAGES_PER_DATASET:
        logger.warning("BdP dataset %s/%s hit page cap (%d); truncating", domain_id, dataset_id, MAX_PAGES_PER_DATASET)
    return series


# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------


def _rows_for_dataset(
    *,
    domain_id: str,
    domain_name: str,
    stub: dict[str, Any],
    series_stubs: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Emit a ``dataset`` row + one ``series`` row per crawled stub."""
    ext_raw = stub.get("extension")
    ext: dict[str, Any] = ext_raw if isinstance(ext_raw, dict) else {}
    dataset_id = _catalog.clean(ext.get("id"))
    if not dataset_id:
        return []
    dataset_label = _catalog.clean(stub.get("label") or ext.get("label") or dataset_id)
    declared = int(ext.get("num_series") or 0)
    last_update = _catalog.clean(ext.get("obs_updated_at"))

    # Per-dataset completeness self-check.
    if declared and len(series_stubs) != declared:
        logger.warning(
            "BdP dataset %s/%s: crawled %d series, stub declares %d",
            domain_id,
            dataset_id,
            len(series_stubs),
            declared,
        )

    rows: list[dict[str, str]] = [
        _catalog.dataset_row(
            domain_id=domain_id,
            domain_name=domain_name,
            dataset_id=dataset_id,
            dataset_label=dataset_label,
            num_series=declared or len(series_stubs),
            last_update=last_update,
        )
    ]
    for s in series_stubs:
        sid = _catalog.clean(s.get("id"))
        if not sid:
            continue
        rows.append(
            _catalog.series_row(
                domain_id=domain_id,
                domain_name=domain_name,
                dataset_id=dataset_id,
                dataset_label=dataset_label,
                series_id=sid,
                label=_catalog.clean(s.get("label") or sid),
                last_update=last_update,
            )
        )
    return rows


@enumerator(output=BDP_ENUMERATE_OUTPUT, tags=["macro", "pt"])
def enumerate_bdp() -> pd.DataFrame:
    """Enumerate Banco de Portugal domains, datasets, and paginated series.

    Walks the 65 leaf domains, paginates each domain's datasets list, and crawls
    each dataset's series (``page_size=100&obs_last_n=1``) with transient-error
    retries. Returns the exact ``BDP_ENUMERATE_OUTPUT`` columns (synthetic
    ``domain:`` / ``dataset:`` rows plus ``{domain}:{dataset}:{series}`` series
    rows); descriptions are crawl-only here and enriched bilingually at
    catalog-build time.
    """
    rows: list[dict[str, str]] = []
    failed: list[str] = []

    with httpx.Client(
        timeout=httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0),
        headers=HEADERS,
        follow_redirects=True,
    ) as client:
        fetcher = ThrottledJsonFetcher(client, provider="bdp", config=METADATA_CRAWL, logger=logger)

        domains = _list_domains(fetcher)
        if not domains:
            logger.warning("BdP enumerate: /domains fetch failed; emitting empty catalog")
            return pd.DataFrame(columns=list(ENUMERATE_COLUMNS))

        leaf_domains = [d for d in domains if d.get("has_series")]
        logger.info("BdP enumerate: %d domains, %d leaf (has_series)", len(domains), len(leaf_domains))

        for d in leaf_domains:
            did = _catalog.clean(d.get("id"))
            if not did:
                continue
            name = _catalog.clean(d.get("label") or d.get("description") or did)
            rows.append(
                _catalog.domain_row(
                    domain_id=did,
                    name=name,
                    description=_catalog.clean(d.get("description")),
                    num_series=int(d.get("num_series") or 0),
                    num_datasets=int(d.get("num_datasets") or 0),
                    last_update=_catalog.clean(d.get("obs_updated_at")),
                )
            )

        # Discover datasets per leaf domain (serial; fetcher throttles internally).
        domain_datasets = [(domain, _list_datasets(fetcher, domain.get("id", ""))) for domain in leaf_domains]

        work: list[tuple[str, str, dict[str, Any]]] = []
        for domain, stubs in domain_datasets:
            did = _catalog.clean(domain.get("id"))
            name = _catalog.clean(domain.get("label") or domain.get("description") or did)
            for stub in stubs:
                work.append((did, name, stub))
        logger.info("BdP enumerate: %d datasets across leaf domains", len(work))

        # Per-dataset crawl: walk each dataset's series pages in turn.
        for did, name, stub in work:
            ext_raw = stub.get("extension")
            ext: dict[str, Any] = ext_raw if isinstance(ext_raw, dict) else {}
            dataset_id = _catalog.clean(ext.get("id"))
            if not dataset_id:
                continue
            series_stubs = _crawl_dataset_series(fetcher, did, dataset_id)
            if series_stubs is None:
                failed.append(f"{did}/{dataset_id}")
                continue
            rows.extend(_rows_for_dataset(domain_id=did, domain_name=name, stub=stub, series_stubs=series_stubs))

    if failed:
        logger.info("BdP enumerate: %d datasets failed: %s", len(failed), ", ".join(failed[:20]))
    n_series = sum(1 for r in rows if r["entity_type"] == "series")
    logger.info("BdP enumerate: emitted %d rows (%d series)", len(rows), n_series)

    columns = list(ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
