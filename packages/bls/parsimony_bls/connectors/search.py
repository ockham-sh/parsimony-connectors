"""``bls_surveys_search`` and ``bls_series_search`` -- the two-tier discovery tools.

**Expected usage (tier-1 -> tier-2 -> fetch):**

1. ``bls_surveys_search(query=...)`` on ``bls_surveys`` -- find the survey and read
   its ``dimensions`` manifest (codes + labels).
2. ``bls_series_search(survey='CU', query=...)`` -- search that survey's series
   (lexical title or structured ``FIELD: value`` dimension clauses).
3. ``bls_fetch(series_id=..., start_year=..., end_year=...)``.

Catalogs are loaded from a published snapshot when present, else built on demand
from the live flat files and cached in an LRU (the SDMX pattern). The GB-scale
microdata surveys are not indexable on demand -- ``bls_series_search`` raises with
guidance to construct an id from the manifest and ``bls_fetch`` it.
"""

from __future__ import annotations

import os

import pandas as pd
from parsimony.catalog import Catalog
from parsimony.catalog.search import CatalogLRU, resolved_catalog_url
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError

from parsimony_bls.catalog_build import build_series_catalog, build_surveys_catalog
from parsimony_bls.outputs import BLS_SERIES_SEARCH_OUTPUT, BLS_SURVEYS_SEARCH_OUTPUT
from parsimony_bls.surveys import SURVEYS_NAMESPACE, normalize_survey, series_namespace

PARSIMONY_BLS_CATALOG_URL_ENV = "PARSIMONY_BLS_CATALOG_URL"
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/bls"
DEFAULT_LRU_SIZE = 4


def _lru_size_from_env() -> int:
    raw = os.environ.get("PARSIMONY_BLS_CATALOG_LRU_SIZE", "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


_lru = CatalogLRU(_lru_size_from_env())


def _get_or_load_catalog(namespace: str, *, catalog_root: str | None = None, build=None) -> Catalog:
    root = resolved_catalog_url(PARSIMONY_BLS_CATALOG_URL_ENV, DEFAULT_CATALOG_ROOT, override=catalog_root)
    url = f"{root}/{namespace}"
    cache_path = lazy_catalog_dir("bls", namespace)
    return _lru.get_or_load(url, cache_path=cache_path, build=build)


def _clear_catalog_lru() -> None:
    _lru.clear()


@connector(output=BLS_SURVEYS_SEARCH_OUTPUT, tags=["macro", "us", "tool"])
def bls_surveys_search(query: str, limit: int = 10, catalog_root: str | None = None) -> pd.DataFrame:
    """Discover BLS surveys and read their dimension manifests.

    Searches the ``bls_surveys`` catalog (survey name text or ``code: CU``), returning a
    relevance-ranked top-N. The ``dimensions`` column lists each dimension's codes + labels
    for surveys with a published series catalog -- use it to pick a survey and to build the
    exact ``filters=`` for ``bls_series_search``, then ``bls_fetch``.
    """

    def _build() -> Catalog:
        return build_surveys_catalog()

    catalog = _get_or_load_catalog(SURVEYS_NAMESPACE, catalog_root=catalog_root, build=_build)
    matches = catalog.search(query, limit=limit)
    if not matches:
        raise EmptyDataError("bls", message=f"No survey matches for query={query!r}.")

    rows: list[dict[str, object]] = []
    for m in matches:
        meta = m.metadata or {}
        dimensions = meta.get("dimensions", [])
        rows.append(
            {
                "code": m.code,
                "title": m.title,
                "survey": str(meta.get("survey", m.code)),
                "dimensions": dimensions if isinstance(dimensions, list) else [],
                "coverage": round(m.coverage, 6),
                "score": round(m.score, 6),
                "matched": m.matched,
            }
        )
    return pd.DataFrame(rows)


@connector(output=BLS_SERIES_SEARCH_OUTPUT, tags=["macro", "us", "tool"])
def bls_series_search(
    query: str,
    survey: str,
    limit: int = 10,
    filters: dict[str, str] | None = None,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Search one survey's series as a relevance-ranked top-N (NOT the full survey).

    ``query`` soft-ranks series titles. ``filters`` is an exact AND constraint on series
    metadata columns that *excludes* non-matching variants (``query`` alone only re-ranks
    them) — use a dimension's code column (``item_code``, ``area_code``, … from the
    ``bls_surveys_search`` manifest) or a raw column like ``seasonal`` (``S``/``U``) to pin
    the exact SA/area/item variant. Pass ``query=""`` to enumerate purely by filter.

    To walk a whole dimension, use the ``bls_surveys_search`` manifest. ``survey`` is a BLS
    abbreviation (e.g. ``CU``). Chain: ``bls_surveys_search`` -> ``bls_series_search`` ->
    ``bls_fetch``; a hit's ``series_id`` goes straight to ``bls_fetch``.
    """
    sv = normalize_survey(survey)
    namespace = series_namespace(sv)
    q = query.strip() or None
    filter_spec = {col: [str(val)] for col, val in filters.items()} if filters else None
    if q is None and not filter_spec:
        raise InvalidParameterError("bls", "bls_series_search requires query= and/or filters=.")

    def _build() -> Catalog:
        return build_series_catalog(sv)

    catalog = _get_or_load_catalog(namespace, catalog_root=catalog_root, build=_build)
    matches = catalog.search(q, limit=limit, filter=filter_spec)
    if not matches:
        raise EmptyDataError(
            "bls",
            message=(
                f"No series matches for query={query!r} filters={filters!r} in survey={sv!r} "
                f"(namespace={namespace}). Check dimension codes via bls_surveys_search first."
            ),
        )

    return pd.DataFrame(
        [
            {
                "series_id": m.code,
                "title": m.title,
                "survey": sv,
                "namespace": m.namespace,
                "coverage": round(m.coverage, 6),
                "score": round(m.score, 6),
                "matched": m.matched,
            }
            for m in matches
        ]
    )


__all__ = [
    "DEFAULT_CATALOG_ROOT",
    "PARSIMONY_BLS_CATALOG_URL_ENV",
    "_clear_catalog_lru",
    "bls_series_search",
    "bls_surveys_search",
]
