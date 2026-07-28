"""``bls_surveys_search`` and ``bls_series_search`` -- the two-tier discovery tools.

**Expected usage (tier-1 -> tier-2 -> fetch):**

1. ``bls_surveys_search(query=...)`` on ``bls_surveys`` -- find the survey and read
   its ``dimensions`` manifest (codes + labels).
2. ``bls_series_search(survey='CU', query=...)`` -- rank that survey's series by
   literal title text, and/or pin exact dimension codes with ``filter=``.
3. ``bls_fetch(series_id=..., start_year=..., end_year=...)``.

Catalogs are loaded from a published snapshot when present, else built on demand
from the live flat files and cached in an LRU (the SDMX pattern). The GB-scale
microdata surveys are not indexable on demand -- ``bls_series_search`` raises with
guidance to construct an id from the manifest and ``bls_fetch`` it.
"""

from __future__ import annotations

import os
import re

import pandas as pd
from parsimony.catalog import Catalog
from parsimony.catalog.search import CatalogLRU, resolved_catalog_url, wire_score, wire_search_detail
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError

from parsimony_bls.catalog_build import build_series_catalog, build_surveys_catalog
from parsimony_bls.outputs import BLS_SERIES_SEARCH_OUTPUT, BLS_SURVEYS_SEARCH_OUTPUT
from parsimony_bls.surveys import SURVEYS_NAMESPACE, normalize_survey, series_namespace

#: The declared search surface for both BLS catalogs. Titles are the curated text;
#: ``code`` is indexed for identifier lookups but is not a relevance surface (a BLS
#: series id is a positional concatenation, not language), and the per-dimension
#: label fields are reached exactly through ``filter=``, not fuzzily.
SEARCH_FIELD = "title"

PARSIMONY_BLS_CATALOG_URL_ENV = "PARSIMONY_BLS_CATALOG_URL"
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/bls"
DEFAULT_LRU_SIZE = 4

#: Short token shapes typical of BLS dimension *codes* (``S``/``U``, ``00000000``, …).
#: Used only to catch the common mistake of filtering a bare dim (labels) with codes.
_CODE_SHAPED = re.compile(r"^[A-Za-z0-9._-]{1,12}$")


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


def _series_filter_fields(catalog: Catalog) -> set[str]:
    """Filterable column names for one survey series catalog."""
    dims = [name for name in catalog.indexes if name not in ("code", "title")]
    known = {"code", "title", "namespace", *dims}
    for dim in dims:
        known.add(f"{dim}_code")
        known.add(f"{dim}_label")
    return known


def _validate_series_filter(catalog: Catalog, filter: dict[str, str | list[str]]) -> None:
    """Reject unknown filter keys; steer code-shaped values off bare dim (label) keys."""
    known = _series_filter_fields(catalog)
    dims = {name for name in catalog.indexes if name not in ("code", "title")}
    for field, raw in filter.items():
        values = raw if isinstance(raw, list) else [raw]
        if field not in known:
            code_alt = f"{field}_code"
            hint = f"; did you mean {code_alt!r}?" if code_alt in known else f"; valid columns: {sorted(known)}"
            raise InvalidParameterError("bls", f"unknown filter column {field!r}{hint}")
        if (
            field in dims
            and values
            and all(isinstance(v, str) and _CODE_SHAPED.fullmatch(v) is not None and " " not in v for v in values)
        ):
            raise InvalidParameterError(
                "bls",
                f"filter key {field!r} matches dimension *labels*, not codes; "
                f"for codes like {list(values)!r} use {field + '_code'!r}",
            )


@connector(output=BLS_SURVEYS_SEARCH_OUTPUT, tags=["macro", "us", "tool"])
def bls_surveys_search(query: str, limit: int = 10, catalog_root: str | None = None) -> pd.DataFrame:
    """Discover BLS surveys and read their dimension manifests.

    ``query`` is literal text ranked against survey titles — never a
    ``FIELD: value`` expression. The ``dimensions`` column lists each dimension's
    codes + labels for surveys with a published series catalog -- use it to pick a
    survey and to build the exact ``filter=`` for ``bls_series_search``, then
    ``bls_fetch``.
    """

    def _build() -> Catalog:
        return build_surveys_catalog()

    catalog = _get_or_load_catalog(SURVEYS_NAMESPACE, catalog_root=catalog_root, build=_build)
    matches = catalog.search(query, limit=limit, field=SEARCH_FIELD)
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
                "score": wire_score(m.score),
                "search_detail": wire_search_detail(m.search_detail),
            }
        )
    return pd.DataFrame(rows)


@connector(output=BLS_SERIES_SEARCH_OUTPUT, tags=["macro", "us", "tool"])
def bls_series_search(
    query: str,
    survey: str,
    limit: int = 10,
    filter: dict[str, str | list[str]] | None = None,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Search one survey's series as a relevance-ranked top-N (NOT the full survey).

    ``query`` is literal text that soft-ranks series titles. ``filter`` is an exact
    AND constraint that *excludes* non-matching variants (``query`` alone only
    re-ranks them) — pin with dimension **code** columns (``item_code``,
    ``area_code``, ``seasonal_code``, … — values like ``S``/``U``) from the
    ``bls_surveys_search`` manifest. Bare dimension names (``seasonal``, ``item``, …)
    filter *labels*, not codes. A list means "any of these". ``query=""`` enumerates
    by filter alone. ``survey`` is a BLS abbreviation (e.g. ``CU``). Chain:
    ``bls_surveys_search`` -> ``bls_series_search`` -> ``bls_fetch``.
    """
    sv = normalize_survey(survey)
    namespace = series_namespace(sv)
    q = query.strip() or None
    if q is None and not filter:
        raise InvalidParameterError("bls", "bls_series_search requires query= and/or filter=.")

    def _build() -> Catalog:
        return build_series_catalog(sv)

    catalog = _get_or_load_catalog(namespace, catalog_root=catalog_root, build=_build)
    if filter:
        _validate_series_filter(catalog, filter)
    matches = catalog.search(q, limit=limit, filter=filter, field=SEARCH_FIELD if q else None)
    if not matches:
        raise EmptyDataError(
            "bls",
            message=(
                f"No series matches for query={query!r} filter={filter!r} in survey={sv!r} "
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
                "score": wire_score(m.score),
                "search_detail": wire_search_detail(m.search_detail),
            }
            for m in matches
        ]
    )


__all__ = [
    "DEFAULT_CATALOG_ROOT",
    "SEARCH_FIELD",
    "PARSIMONY_BLS_CATALOG_URL_ENV",
    "_clear_catalog_lru",
    "bls_series_search",
    "bls_surveys_search",
]
