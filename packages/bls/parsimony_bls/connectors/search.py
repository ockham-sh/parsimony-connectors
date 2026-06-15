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
from parsimony.errors import EmptyDataError

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

    Searches the ``bls_surveys`` catalog (survey name text or ``code: CU``). The
    ``dimensions`` column lists each dimension's codes + labels for surveys with a
    published series catalog -- use it to pick a survey, then ``bls_series_search``.
    """

    def _build() -> Catalog:
        return build_surveys_catalog()

    catalog = _get_or_load_catalog(SURVEYS_NAMESPACE, catalog_root=catalog_root, build=_build)
    matches, _ = catalog.search(query, limit=limit)
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
                "score": round(m.score, 6),
                "survey": str(meta.get("survey", m.code)),
                "dimensions": dimensions if isinstance(dimensions, list) else [],
            }
        )
    return pd.DataFrame(rows)


@connector(output=BLS_SERIES_SEARCH_OUTPUT, tags=["macro", "us", "tool"])
def bls_series_search(
    query: str, survey: str, limit: int = 10, catalog_root: str | None = None
) -> pd.DataFrame:
    """Search one survey's series catalog (structured dimension clauses preferred).

    ``survey`` is a BLS abbreviation (e.g. ``CU``). Chain:
    ``bls_surveys_search`` -> ``bls_series_search`` -> ``bls_fetch``. A series hit's
    ``series_id`` goes straight to ``bls_fetch``.
    """
    sv = normalize_survey(survey)
    namespace = series_namespace(sv)

    def _build() -> Catalog:
        return build_series_catalog(sv)

    catalog = _get_or_load_catalog(namespace, catalog_root=catalog_root, build=_build)
    matches, _ = catalog.search(query, limit=limit)
    if not matches:
        raise EmptyDataError(
            "bls",
            message=(
                f"No series matches for query={query!r} in survey={sv!r} "
                f"(namespace={namespace}). Try bls_surveys_search first."
            ),
        )

    return pd.DataFrame(
        [
            {
                "series_id": m.code,
                "title": m.title,
                "score": round(m.score, 6),
                "survey": sv,
                "namespace": m.namespace,
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
