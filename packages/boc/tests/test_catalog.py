"""Tests for BoC catalog loading and BM25 fallback."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest
import respx
from parsimony.errors import ConnectorError

from parsimony_boc import boc_search, load
from parsimony_boc.catalog import _clear_catalog_cache_for_tests, get_catalog


def _mock_minimal_enumeration() -> None:
    respx.get("https://www.bankofcanada.ca/valet/lists/series/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "series": {
                    "FXUSDCAD": {
                        "label": "USD/CAD",
                        "description": "US dollar to Canadian dollar daily exchange rate",
                    },
                }
            },
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/lists/groups/json").mock(
        return_value=httpx.Response(200, json={"groups": {}})
    )


@pytest.mark.asyncio
async def test_get_catalog_raises_without_fallback_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_catalog_cache_for_tests()
    monkeypatch.setattr(
        "parsimony_boc.catalog.Catalog.load",
        AsyncMock(side_effect=FileNotFoundError("missing snapshot")),
    )

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        await get_catalog(catalog_url="file:///tmp/missing-boc", fallback_bm25=False)


@pytest.mark.asyncio
async def test_get_catalog_raises_on_integrity_failure_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_catalog_cache_for_tests()
    monkeypatch.setattr(
        "parsimony_boc.catalog.Catalog.load",
        AsyncMock(
            side_effect=ValueError(
                "Catalog snapshot integrity check failed for /tmp/boc: expected sha256: abc actual: def"
            )
        ),
    )

    with pytest.raises(ConnectorError, match="integrity validation"):
        await get_catalog(catalog_url="file:///tmp/boc", fallback_bm25=True)


@respx.mock
@pytest.mark.asyncio
async def test_get_catalog_builds_bm25_fallback_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    from parsimony_boc import enumerate_boc

    _clear_catalog_cache_for_tests()
    _mock_minimal_enumeration()
    monkeypatch.setattr(
        "parsimony_boc.catalog.Catalog.load",
        AsyncMock(side_effect=FileNotFoundError("missing snapshot")),
    )

    catalog = await get_catalog(
        catalog_url="file:///tmp/missing-boc",
        fallback_bm25=True,
        enumerate=lambda: enumerate_boc(),
    )

    assert catalog.name == "boc"
    assert len(catalog) >= 1
    matches, _ = await catalog.search("USD", limit=5)
    assert len(matches) >= 1
    assert matches[0].code == "FXUSDCAD"


def test_load_sets_runtime_defaults() -> None:
    from parsimony_boc.catalog import _runtime, configure

    configure(catalog_url=None, fallback_bm25=False)
    runtime = load(catalog_url="file:///tmp/boc", fallback_bm25=True)
    assert runtime is not None
    assert _runtime.catalog_url == "file:///tmp/boc"
    assert _runtime.fallback_bm25 is True


@respx.mock
@pytest.mark.asyncio
async def test_boc_search_uses_bm25_fallback_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_catalog_cache_for_tests()
    _mock_minimal_enumeration()
    monkeypatch.setattr(
        "parsimony_boc.catalog.Catalog.load",
        AsyncMock(side_effect=FileNotFoundError("missing snapshot")),
    )

    result = await boc_search(
        query="USD",
        catalog_url="file:///tmp/missing-boc",
        fallback_bm25=True,
    )
    df = result.data

    assert not df.empty
    assert "FXUSDCAD" in set(df["code"])
