"""BoJ catalog search connectors (multi-bundle)."""

from __future__ import annotations

import logging
import os

import pandas as pd
from parsimony.catalog.search import CatalogLRU, resolved_catalog_url
from parsimony.catalog.source import lazy_catalog_dir
from parsimony.connector import connector
from parsimony.errors import EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_boj.catalog_build import (
    DATABASES_NAMESPACE,
    DEFAULT_CATALOG_ROOT,
    build_boj_databases_catalog_from_enumeration,
    build_boj_series_catalog_for_db,
    series_namespace,
)

logger = logging.getLogger(__name__)

PARSIMONY_BOJ_CATALOG_URL_ENV = "PARSIMONY_BOJ_CATALOG_URL"
DEFAULT_LRU_SIZE = 4


def _lru_size_from_env() -> int:
    raw = os.environ.get("PARSIMONY_BOJ_CATALOG_LRU_SIZE", "")
    try:
        n = int(raw) if raw else DEFAULT_LRU_SIZE
    except ValueError:
        return DEFAULT_LRU_SIZE
    return max(1, n)


_lru = CatalogLRU(_lru_size_from_env())


def _clear_catalog_lru() -> None:
    """Test helper: drop cached catalog bundles."""

    _lru.clear()


def _get_or_load_catalog(namespace: str, *, catalog_root: str | None = None, build=None):
    root = resolved_catalog_url(
        PARSIMONY_BOJ_CATALOG_URL_ENV,
        DEFAULT_CATALOG_ROOT,
        override=catalog_root,
    )
    url = f"{root}/{namespace}"
    cache_path = lazy_catalog_dir("boj", namespace)
    return _lru.get_or_load(url, cache_path=cache_path, build=build)


def _normalize_db(db: str) -> str:
    return db.strip().upper()


DATABASES_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="db", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(
            name="series_namespace",
            role=ColumnRole.METADATA,
            description="Namespace for boj_series_search, e.g. boj_series_fm08.",
        ),
    ]
)


class DatabasesSearchParams(BaseModel):
    query: str = Field(min_length=1, max_length=512)
    limit: int = Field(default=10, ge=1, le=50)
    catalog_root: str | None = None


@connector(output=DATABASES_SEARCH_OUTPUT, tags=["macro", "jp", "tool"])
def boj_databases_search(
    query: str,
    limit: int = 10,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Search BoJ statistics databases (step 1 of catalog discovery).

    Returns ``db`` codes for ``boj_series_search`` and ``series_namespace`` hints.
    Chain: ``boj_databases_search`` → ``boj_series_search(db=...)`` → ``boj_fetch``.
    """
    params = DatabasesSearchParams(query=query, limit=limit, catalog_root=catalog_root)
    catalog = _get_or_load_catalog(
        DATABASES_NAMESPACE,
        catalog_root=params.catalog_root,
        build=build_boj_databases_catalog_from_enumeration,
    )
    matches = catalog.search(params.query, limit=params.limit)
    if not matches:
        raise EmptyDataError(
            provider="boj",
            message=f"No database matches for query={params.query!r}.",
        )
    rows = []
    for m in matches:
        db_code = m.code
        category = str(m.metadata.get("category") or "") if m.metadata else ""
        rows.append(
            {
                "db": db_code,
                "title": m.title,
                "score": round(m.score, 6),
                "category": category,
                "series_namespace": series_namespace(db_code),
            }
        )
    return pd.DataFrame(rows)


SERIES_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="score", role=ColumnRole.DATA),
        Column(name="db", role=ColumnRole.METADATA),
    ]
)


class SeriesSearchParams(BaseModel):
    query: str = Field(min_length=1, max_length=512)
    db: str = Field(min_length=1, max_length=16, description="Statistics database code, e.g. FM08.")
    limit: int = Field(default=10, ge=1, le=50)
    catalog_root: str | None = None


@connector(output=SERIES_SEARCH_OUTPUT, tags=["macro", "jp", "tool"])
def boj_series_search(
    query: str,
    db: str,
    limit: int = 10,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Search series within one BoJ statistics database.

    Requires ``db`` from ``boj_databases_search``. Returns ``code`` and ``db`` columns.
    Dispatch: ``boj_fetch(db=r["db"], code=r["code"])``.
    """
    params = SeriesSearchParams(query=query, db=db, limit=limit, catalog_root=catalog_root)
    db_code = _normalize_db(params.db)
    namespace = series_namespace(db_code)

    def _build():
        return build_boj_series_catalog_for_db(db_code)

    catalog = _get_or_load_catalog(namespace, catalog_root=params.catalog_root, build=_build)
    matches = catalog.search(params.query, limit=params.limit)
    if not matches:
        raise EmptyDataError(
            provider="boj",
            message=(
                f"No series matches for query={params.query!r} in db={db_code!r}. "
                "Try a broader query or pick another database from boj_databases_search."
            ),
        )
    return pd.DataFrame(
        [
            {
                "code": m.code,
                "title": m.title,
                "score": round(m.score, 6),
                "db": db_code,
            }
            for m in matches
        ]
    )


__all__ = [
    "DATABASES_SEARCH_OUTPUT",
    "DEFAULT_CATALOG_ROOT",
    "PARSIMONY_BOJ_CATALOG_URL_ENV",
    "SERIES_SEARCH_OUTPUT",
    "DatabasesSearchParams",
    "SeriesSearchParams",
    "_clear_catalog_lru",
    "boj_databases_search",
    "boj_series_search",
]
