"""Tests for ``boj_databases_search`` / ``boj_series_search`` connector contract."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from parsimony.errors import EmptyDataError

from parsimony_boj.search import (
    DEFAULT_CATALOG_ROOT,
    PARSIMONY_BOJ_CATALOG_URL_ENV,
    _clear_catalog_lru,
    boj_databases_search,
    boj_series_search,
)

_CATALOG_LOAD = "parsimony.catalog.search.Catalog.load"


@pytest.fixture(autouse=True)
def _reset_lru() -> Iterator[None]:
    _clear_catalog_lru()
    yield
    _clear_catalog_lru()


class _FakeCatalog:
    def __init__(
        self,
        *,
        code: str = "FXERD01",
        title: str = "JPY/USD Spot Rate",
        metadata: dict[str, object] | None = None,
    ) -> None:
        self._code = code
        self._title = title
        self._metadata = metadata or {}

    def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
        from parsimony.catalog import CatalogMatch

        return [
            CatalogMatch(
                namespace="boj_series_fm08",
                code=self._code,
                title=self._title,
                score=0.9,
                metadata=self._metadata,
            )
        ]


def test_connectors_are_tool_tagged() -> None:
    assert "tool" in boj_databases_search.tags
    assert "tool" in boj_series_search.tags


def test_databases_search_output_includes_dispatch_columns() -> None:
    cfg = boj_databases_search.output_spec
    assert cfg is not None
    assert [c.name for c in cfg.columns] == [
        "db",
        "title",
        "score",
        "category",
        "series_namespace",
    ]


def test_series_search_output_includes_db_for_fetch() -> None:
    cfg = boj_series_search.output_spec
    assert cfg is not None
    assert [c.name for c in cfg.columns] == ["code", "title", "score", "db"]


def test_series_search_loads_per_db_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    _clear_catalog_lru()

    result = boj_series_search(query="USD", db="FM08", limit=1)

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/boj_series_fm08"]
    df = result.raw
    assert df.iloc[0]["code"] == "FXERD01"
    assert df.iloc[0]["db"] == "FM08"


def test_databases_search_loads_databases_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog(code="FM08", title="Foreign Exchange Rates", metadata={"category": "Markets"})

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    _clear_catalog_lru()

    result = boj_databases_search(query="exchange", limit=1)

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/boj_databases"]
    df = result.raw
    assert df.iloc[0]["db"] == "FM08"
    assert df.iloc[0]["series_namespace"] == "boj_series_fm08"


def test_env_overrides_catalog_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PARSIMONY_BOJ_CATALOG_URL_ENV, "file:///tmp/local-boj/")
    seen: list[str] = []

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    _clear_catalog_lru()

    boj_series_search(query="USD", db="fm08", limit=1)

    assert seen == ["file:///tmp/local-boj/boj_series_fm08"]


def test_empty_series_search_raises_empty_data(monkeypatch: pytest.MonkeyPatch) -> None:
    class _EmptyCatalog:
        def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            return []

    def _spy_load(url: str) -> Any:  # noqa: ARG001
        return _EmptyCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    _clear_catalog_lru()

    with pytest.raises(EmptyDataError, match="No series matches"):
        boj_series_search(query="missing", db="FM08", limit=1)
