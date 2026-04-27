"""Tests for ``sdmx_series_search`` / ``sdmx_datasets_search`` MCP tools.

Three concerns:

* **Catalog URL resolution** — env var must be set; namespace concat
  matches the published layout (``{root}/{namespace}``).
* **Flow-id parsing** — accepts 'AGENCY/FLOW', 'AGENCY-FLOW', and
  full-namespace pass-through; bad shapes raise ``ProviderError`` with
  a directive (not a stack trace) so the agent has a recovery path.
* **LRU caching** — repeat search hits the in-memory catalog without
  re-loading from disk; cap evicts oldest.

End-to-end search quality is covered by ``catalogs/sdmx/eval/run_eval.py``;
these tests lock in the connector contract only.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from parsimony.errors import ConnectorError, EmptyDataError

from parsimony_sdmx.connectors import search as search_module
from parsimony_sdmx.connectors.search import (
    PARSIMONY_SDMX_CATALOG_ROOT_ENV,
    SeriesSearchParams,
    _catalog_url,
    _clear_catalog_lru,
    _resolve_series_namespace,
    sdmx_series_search,
)


@pytest.fixture(autouse=True)
def _reset_lru() -> None:
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
        assert (
            _resolve_series_namespace("sdmx_series_ecb_hicp")
            == "sdmx_series_ecb_hicp"
        )

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

    def test_bare_token_defaults_to_ecb_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        with caplog.at_level(logging.WARNING):
            ns = _resolve_series_namespace("HICP")
        assert ns == "sdmx_series_ecb_hicp"
        assert any(
            "no agency separator" in rec.message for rec in caplog.records
        )


# ---------------------------------------------------------------------------
# Catalog URL resolution
# ---------------------------------------------------------------------------


class TestCatalogUrl:
    def test_file_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_ROOT_ENV, "file:///tmp/repo")
        assert (
            _catalog_url("sdmx_series_ecb_hicp")
            == "file:///tmp/repo/sdmx_series_ecb_hicp"
        )

    def test_hf_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_ROOT_ENV, "hf://ockham")
        assert (
            _catalog_url("sdmx_series_ecb_hicp")
            == "hf://ockham/sdmx_series_ecb_hicp"
        )

    def test_trailing_slash_stripped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_ROOT_ENV, "file:///tmp/repo/")
        assert (
            _catalog_url("sdmx_series_ecb_hicp")
            == "file:///tmp/repo/sdmx_series_ecb_hicp"
        )

    def test_unset_raises_provider_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(PARSIMONY_SDMX_CATALOG_ROOT_ENV, raising=False)
        with pytest.raises(ConnectorError, match="DO NOT retry"):
            _catalog_url("sdmx_series_ecb_hicp")

    def test_empty_raises_provider_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_ROOT_ENV, "   ")
        with pytest.raises(ConnectorError):
            _catalog_url("sdmx_series_ecb_hicp")


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
    assert names == ["series_key", "title", "similarity", "namespace"]


# ---------------------------------------------------------------------------
# LRU caching with a faked Catalog.from_url
# ---------------------------------------------------------------------------


def test_lru_caches_catalog_across_searches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two searches against the same flow → exactly one ``from_url`` call.

    The on-disk loading + FAISS path is exercised end-to-end by the
    eval harness (``catalogs/sdmx/eval/run_eval.py``); this test only
    locks in the LRU contract.
    """

    class _FakeCatalog:
        async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            from parsimony.catalog import SeriesMatch

            return [
                SeriesMatch(
                    namespace="sdmx_series_ecb_test",
                    code="A.1",
                    title="stub",
                    similarity=0.5,
                )
            ]

    load_calls: list[str] = []

    async def _spy_from_url(url, *, embedder=None):  # noqa: ARG001
        load_calls.append(url)
        return _FakeCatalog()

    monkeypatch.setenv(
        PARSIMONY_SDMX_CATALOG_ROOT_ENV, "file:///tmp/fake-repo"
    )
    monkeypatch.setattr(search_module.Catalog, "from_url", _spy_from_url)

    df1 = asyncio.run(
        sdmx_series_search(
            SeriesSearchParams(
                query="HICP", flow_id="ECB/test", limit=1
            )
        )
    )
    df2 = asyncio.run(
        sdmx_series_search(
            SeriesSearchParams(
                query="HICP again", flow_id="ECB/test", limit=1
            )
        )
    )

    assert len(load_calls) == 1
    # Connector returns a Result whose ``columns`` are Column objects
    # carrying the declared schema; ``df`` materializes the underlying frame.
    col_names = [c.name for c in df1.columns]
    assert col_names == ["series_key", "title", "similarity", "namespace"]
    assert df1.df["series_key"].iloc[0] == "A.1"
    assert df2.df["series_key"].iloc[0] == "A.1"


def test_unset_env_raises_with_directive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PARSIMONY_SDMX_CATALOG_ROOT_ENV, raising=False)
    with pytest.raises(ConnectorError, match="DO NOT retry"):
        asyncio.run(
            sdmx_series_search(
                SeriesSearchParams(
                    query="HICP", flow_id="ECB/test", limit=1
                )
            )
        )


def test_empty_search_results_raise_empty_data_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero matches should raise ``EmptyDataError`` with a recovery
    directive — silent empty list would mislead the agent."""

    class _EmptyCatalog:
        async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            return []

    async def _from_url(url, *, embedder=None):  # noqa: ARG001
        return _EmptyCatalog()

    monkeypatch.setenv(
        PARSIMONY_SDMX_CATALOG_ROOT_ENV, "file:///tmp/fake-repo"
    )
    monkeypatch.setattr(search_module.Catalog, "from_url", _from_url)

    with pytest.raises(EmptyDataError, match="No matches"):
        asyncio.run(
            sdmx_series_search(
                SeriesSearchParams(
                    query="nonsense", flow_id="ECB/test", limit=1
                )
            )
        )
