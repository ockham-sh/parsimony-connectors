"""Tests for ``sdmx_datasets_search`` / ``sdmx_codelist_search`` MCP tools."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from parsimony.catalog import Catalog, Entity
from parsimony.errors import EmptyDataError, InvalidParameterError

from parsimony_sdmx.connectors import search as search_module
from parsimony_sdmx.connectors.search import (
    DEFAULT_CATALOG_ROOT,
    PARSIMONY_SDMX_CATALOG_URL_ENV,
    _clear_catalog_lru,
    sdmx_codelist_search,
    sdmx_datasets_search,
)


@pytest.fixture(autouse=True)
def _reset_lru() -> Iterator[None]:
    _clear_catalog_lru()
    yield
    _clear_catalog_lru()


def _catalog_with_entities(namespace: str, entities: list[Entity]) -> Catalog:
    catalog = Catalog(namespace)
    catalog.set_entities(entities)
    catalog.build()
    return catalog


def test_sdmx_datasets_search_agency_optional_fanout(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_get_or_load(namespace: str, **kwargs: Any) -> Catalog:
        calls.append(namespace)
        return _catalog_with_entities(
            namespace,
            [
                Entity(
                    namespace=namespace,
                    code=f"{namespace}|FLOW",
                    title=f"Title for {namespace}",
                    metadata={
                        "agency": namespace.removeprefix("sdmx_datasets_").upper(),
                        "dataset_id": "FLOW",
                        "dsd": [{"dimension_id": "FREQ", "codelist_id": "CL_FREQ"}],
                    },
                )
            ],
        )

    monkeypatch.setattr(search_module, "_get_or_load_catalog", fake_get_or_load)
    df = sdmx_datasets_search(query="Title", limit=2).data
    assert len(df) == 2
    assert "dsd" in df.columns
    assert len(calls) == 4


def test_sdmx_datasets_search_single_agency(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_or_load(namespace: str, **kwargs: Any) -> Catalog:
        return _catalog_with_entities(
            namespace,
            [
                Entity(
                    namespace=namespace,
                    code="ECB|YC",
                    title="Yield curve",
                    metadata={"agency": "ECB", "dataset_id": "YC", "dsd": []},
                )
            ],
        )

    monkeypatch.setattr(search_module, "_get_or_load_catalog", fake_get_or_load)
    df = sdmx_datasets_search(query="yield", agency="ECB", limit=5).data
    assert df.iloc[0]["flow_id"] == "ECB/YC"


def test_sdmx_datasets_search_empty_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        search_module,
        "_get_or_load_catalog",
        lambda namespace, **kwargs: _catalog_with_entities(namespace, []),
    )
    with pytest.raises(EmptyDataError):
        sdmx_datasets_search(query="nonsense", agency="ECB")


def test_sdmx_codelist_search_returns_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    ns = "sdmx_codelist_ecb_cl_freq"

    def fake_get_or_load(namespace: str, **kwargs: Any) -> Catalog:
        return _catalog_with_entities(
            ns,
            [Entity(namespace=ns, code="DE", title="Germany", metadata={"label": "Germany"})],
        )

    monkeypatch.setattr(search_module, "_get_or_load_catalog", fake_get_or_load)
    df = sdmx_codelist_search(query="Germany", agency="ECB", codelist_id="CL_FREQ").data
    assert df.iloc[0]["code"] == "DE"


def test_sdmx_codelist_search_is_tool_tagged() -> None:
    assert "tool" in sdmx_codelist_search.tags


def test_set_catalog_lru_size_rejects_zero() -> None:
    with pytest.raises(InvalidParameterError):
        search_module.set_catalog_lru_size(0)


def test_default_catalog_root_is_hf_dev_sdmx() -> None:
    assert DEFAULT_CATALOG_ROOT == "hf://parsimony-dev/sdmx"


def test_catalog_url_env_constant() -> None:
    assert PARSIMONY_SDMX_CATALOG_URL_ENV == "PARSIMONY_SDMX_CATALOG_URL"
