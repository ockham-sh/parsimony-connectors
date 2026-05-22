"""Tests for ``sdmx_series_search`` / ``sdmx_datasets_search`` MCP tools.

Three concerns:

* **Flow-id parsing** — accepts 'AGENCY/FLOW', 'AGENCY-FLOW', and
  full-namespace pass-through; bad shapes raise ``ConnectorError`` with
  a directive (not a stack trace) so the agent has a recovery path.
* **Catalog loading** — every namespace resolves to
  ``{DEFAULT_CATALOG_ROOT}/{namespace}`` and is loaded via
  ``Catalog.load`` (kernel handles the ``hf://org/repo/sub``
  multi-bundle layout). LRU caches loaded catalogs across calls.
* **Failure mapping** — kernel ``RepositoryNotFoundError`` /
  ``FileNotFoundError`` are wrapped into ``ConnectorError`` with a
  directive so unpublished bundles don't leak as stack traces.

End-to-end search quality is covered by ``catalogs/sdmx/eval/run_eval.py``;
these tests lock in the connector contract only.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest
from huggingface_hub.errors import RepositoryNotFoundError
from parsimony.errors import ConnectorError, EmptyDataError

from parsimony_sdmx.connectors import search as search_module
from parsimony_sdmx.connectors.search import (
    DEFAULT_CATALOG_ROOT,
    PARSIMONY_SDMX_CATALOG_URL_ENV,
    _clear_catalog_lru,
    _resolve_series_namespace,
    sdmx_datasets_search,
    sdmx_series_search,
)

_CATALOG_LOAD = "parsimony.utils.catalog_search.Catalog.load"


@pytest.fixture(autouse=True)
def _reset_lru() -> Iterator[None]:
    _clear_catalog_lru()
    yield
    _clear_catalog_lru()


# ---------------------------------------------------------------------------
# Flow-id parsing
# ---------------------------------------------------------------------------


class TestResolveSeriesNamespace:
    def test_agency_slash_flow(self) -> None:
        assert _resolve_series_namespace("ECB/HICP") == "sdmx_series_ecb_hicp"

    def test_agency_dash_flow(self) -> None:
        assert _resolve_series_namespace("ECB-HICP") == "sdmx_series_ecb_hicp"

    def test_full_namespace_passthrough(self) -> None:
        assert _resolve_series_namespace("sdmx_series_ecb_hicp") == "sdmx_series_ecb_hicp"

    def test_lowercase_agency_normalized(self) -> None:
        assert _resolve_series_namespace("ecb/hicp") == "sdmx_series_ecb_hicp"

    def test_empty_raises_provider_error(self) -> None:
        with pytest.raises(ConnectorError, match="must be non-empty"):
            _resolve_series_namespace("")

    def test_unknown_agency_raises_provider_error(self) -> None:
        with pytest.raises(ConnectorError, match="Unknown agency"):
            _resolve_series_namespace("XYZ/HICP")

    def test_separator_without_dataset_raises(self) -> None:
        with pytest.raises(ConnectorError, match="missing dataset id"):
            _resolve_series_namespace("ECB/")

    def test_bare_token_defaults_to_ecb_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        with caplog.at_level(logging.WARNING):
            ns = _resolve_series_namespace("HICP")
        assert ns == "sdmx_series_ecb_hicp"
        assert any("no agency separator" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Connector wiring
# ---------------------------------------------------------------------------


def test_sdmx_series_search_is_tool_tagged() -> None:
    """MCP exposes the connector iff it carries the ``"tool"`` tag."""
    assert "tool" in sdmx_series_search.tags


def test_sdmx_series_search_carries_output_schema() -> None:
    """The output schema is what the MCP bridge reads to format results."""
    cfg = sdmx_series_search.output_config
    assert cfg is not None
    names = [c.name for c in cfg.columns]
    assert names == ["series_key", "title", "score", "namespace"]


def test_sdmx_datasets_search_carries_dimensions_column() -> None:
    cfg = sdmx_datasets_search.output_config
    assert cfg is not None
    names = [c.name for c in cfg.columns]
    assert names == ["flow_id", "title", "score", "agency", "dataset_id", "dimensions"]


def test_default_catalog_root_is_hf_parsimony_dev_sdmx() -> None:
    """Smoke: the canonical catalog URL is pinned and not env-driven."""
    assert DEFAULT_CATALOG_ROOT == "hf://parsimony-dev/sdmx"


# ---------------------------------------------------------------------------
# Catalog loading + LRU caching
# ---------------------------------------------------------------------------


class _FakeCatalog:
    def __init__(
        self,
        *,
        code: str = "A.1",
        title: str = "stub",
        metadata: dict[str, object] | None = None,
    ) -> None:
        self._code = code
        self._title = title
        self._metadata = metadata or {}
        self.indexes: list[Any] = []

    async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
        from parsimony.catalog import CatalogMatch

        return [
            CatalogMatch(
                namespace="sdmx_series_ecb_test",
                code=self._code,
                title=self._title,
                score=0.5,
                metadata=self._metadata,
            )
        ], []


def test_load_uses_default_root_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """No env var → connector composes ``{DEFAULT_CATALOG_ROOT}/{namespace}``
    and delegates to ``Catalog.load``."""
    monkeypatch.delenv(PARSIMONY_SDMX_CATALOG_URL_ENV, raising=False)
    seen: list[str] = []

    async def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    asyncio.run(sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1))

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/sdmx_series_ecb_test"]


def test_env_overrides_default_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """``PARSIMONY_SDMX_CATALOG_URL`` overrides the default — useful for
    pointing at a local snapshot during catalog dev."""
    monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_URL_ENV, "file:///tmp/local-sdmx/")
    seen: list[str] = []

    async def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    asyncio.run(sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1))

    # Trailing slash on the env value is stripped before composition.
    assert seen == ["file:///tmp/local-sdmx/sdmx_series_ecb_test"]


def test_lru_caches_catalog_across_searches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two searches against the same flow → exactly one load call.

    The on-disk loading path is exercised end-to-end by the
    eval harness (``catalogs/sdmx/eval/run_eval.py``); this test only
    locks in the LRU contract.
    """
    load_calls: list[str] = []

    async def _spy_load(url: str) -> Any:
        load_calls.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    df1 = asyncio.run(sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1))
    df2 = asyncio.run(sdmx_series_search(query="HICP again", flow_id="ECB/test", limit=1))

    assert len(load_calls) == 1
    col_names = list(df1.data.columns)
    assert col_names == ["series_key", "title", "score", "namespace"]
    assert df1.data["series_key"].iloc[0] == "A.1"
    assert df2.data["series_key"].iloc[0] == "A.1"


def test_series_search_uses_loaded_catalog_default_ranker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Series search relies on the serialized catalog default ranker."""

    seen: dict[str, Any] = {}

    class _CatalogWithDefaultRanker(_FakeCatalog):
        async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            seen["called"] = True
            return await super().search(query, limit, namespaces=namespaces)

    async def _spy_load(url: str) -> Any:
        return _CatalogWithDefaultRanker()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    asyncio.run(sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1))

    assert seen.get("called") is True


def test_empty_search_results_raise_empty_data_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero matches raise ``EmptyDataError`` with a recovery directive —
    silent empty list would mislead the agent."""

    class _EmptyCatalog:
        indexes: list[Any] = []

        async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            return [], []

    async def _spy_load(url: str) -> Any:
        return _EmptyCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    with pytest.raises(EmptyDataError, match="No matches"):
        asyncio.run(sdmx_series_search(query="nonsense", flow_id="ECB/test", limit=1))


# ---------------------------------------------------------------------------
# Kernel-error → ConnectorError wrapping
# ---------------------------------------------------------------------------


def test_repo_not_found_wraps_into_connector_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``RepositoryNotFoundError`` from the kernel is wrapped with a
    recovery directive so the agent has a path forward."""

    async def _raise_repo_not_found(url: str) -> Any:
        raise RepositoryNotFoundError("404 not found")

    monkeypatch.setattr(_CATALOG_LOAD, _raise_repo_not_found)
    search_module._clear_catalog_lru()

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        asyncio.run(sdmx_series_search(query="x", flow_id="ECB/never_published", limit=1))


def test_missing_bundle_wraps_into_connector_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``FileNotFoundError`` (kernel ``_load_file`` couldn't find
    ``meta.json``) becomes a directive-bearing ``ConnectorError``."""

    async def _raise_file_not_found(url: str) -> Any:
        raise FileNotFoundError("meta.json not found")

    monkeypatch.setattr(_CATALOG_LOAD, _raise_file_not_found)
    search_module._clear_catalog_lru()

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        asyncio.run(sdmx_series_search(query="x", flow_id="ECB/test", limit=1))


def test_load_binds_catalog_root_on_search_connectors() -> None:
    from parsimony_sdmx import load

    runtime = load(catalog_root="file:///tmp/sdmx-dev")
    series = runtime["sdmx_series_search"]
    datasets = runtime["sdmx_datasets_search"]
    fetch = runtime["sdmx_fetch"]

    assert series.bound_arguments.get("catalog_root") == "file:///tmp/sdmx-dev"
    assert datasets.bound_arguments.get("catalog_root") == "file:///tmp/sdmx-dev"
    assert "catalog_root" not in fetch.bound_arguments


def test_set_catalog_lru_size_rejects_invalid_values() -> None:
    from parsimony_sdmx.connectors.search import set_catalog_lru_size

    with pytest.raises(ValueError, match=">= 1"):
        set_catalog_lru_size(0)


def test_datasets_search_returns_dimensions_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = [{"id": "FREQ", "values": [{"code": "M", "label": "Monthly"}]}]

    async def _spy_load(url: str) -> Any:
        if url.endswith("/sdmx_datasets"):
            return _FakeCatalog(
                code="ECB|YC",
                title="Yield curve",
                metadata={
                    "agency": "ECB",
                    "dataset_id": "YC",
                    "dimensions": manifest,
                },
            )
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    df = asyncio.run(sdmx_datasets_search(query="code: ECB|YC", limit=1))

    assert df.data["flow_id"].iloc[0] == "ECB/YC"
    assert df.data["dimensions"].iloc[0] == manifest


def test_datasets_search_returns_empty_dimensions_for_legacy_catalogs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _spy_load(url: str) -> Any:
        if url.endswith("/sdmx_datasets"):
            return _FakeCatalog(
                code="ECB|YC",
                title="Yield curve",
                metadata={"agency": "ECB", "dataset_id": "YC"},
            )
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    df = asyncio.run(sdmx_datasets_search(query="code: ECB|YC", limit=1))

    assert df.data["dimensions"].iloc[0] == []
