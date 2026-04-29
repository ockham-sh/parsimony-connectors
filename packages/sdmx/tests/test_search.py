"""Tests for ``sdmx_series_search`` / ``sdmx_datasets_search`` MCP tools.

Three concerns:

* **Flow-id parsing** — accepts 'AGENCY/FLOW', 'AGENCY-FLOW', and
  full-namespace pass-through; bad shapes raise ``ConnectorError`` with
  a directive (not a stack trace) so the agent has a recovery path.
* **Catalog loading** — every namespace resolves to
  ``{DEFAULT_CATALOG_ROOT}/{namespace}`` and is loaded via
  ``Catalog.from_url`` (kernel handles the ``hf://org/repo/sub``
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
    SeriesSearchParams,
    _clear_catalog_lru,
    _resolve_series_namespace,
    sdmx_series_search,
)


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


def test_default_catalog_root_is_hf_parsimony_dev_sdmx() -> None:
    """Smoke: the canonical catalog URL is pinned and not env-driven."""
    assert DEFAULT_CATALOG_ROOT == "hf://parsimony-dev/sdmx"


# ---------------------------------------------------------------------------
# Catalog loading + LRU caching
# ---------------------------------------------------------------------------


class _FakeCatalog:
    def __init__(self, *, code: str = "A.1", title: str = "stub") -> None:
        self._code = code
        self._title = title

    async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
        from parsimony.catalog import SeriesMatch

        return [
            SeriesMatch(
                namespace="sdmx_series_ecb_test",
                code=self._code,
                title=self._title,
                similarity=0.5,
            )
        ]


def test_load_uses_default_root_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """No env var → connector composes ``{DEFAULT_CATALOG_ROOT}/{namespace}``
    and delegates to ``Catalog.from_url``."""
    monkeypatch.delenv(PARSIMONY_SDMX_CATALOG_URL_ENV, raising=False)
    seen: list[str] = []

    async def _spy_from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(search_module.Catalog, "from_url", _spy_from_url)

    asyncio.run(
        sdmx_series_search(
            SeriesSearchParams(query="HICP", flow_id="ECB/test", limit=1)
        )
    )

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/sdmx_series_ecb_test"]


def test_env_overrides_default_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """``PARSIMONY_SDMX_CATALOG_URL`` overrides the default — useful for
    pointing at a local snapshot during catalog dev."""
    monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_URL_ENV, "file:///tmp/local-sdmx/")
    seen: list[str] = []

    async def _spy_from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(search_module.Catalog, "from_url", _spy_from_url)

    asyncio.run(
        sdmx_series_search(
            SeriesSearchParams(query="HICP", flow_id="ECB/test", limit=1)
        )
    )

    # Trailing slash on the env value is stripped before composition.
    assert seen == ["file:///tmp/local-sdmx/sdmx_series_ecb_test"]


def test_lru_caches_catalog_across_searches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two searches against the same flow → exactly one ``from_url`` call.

    The on-disk loading + FAISS path is exercised end-to-end by the
    eval harness (``catalogs/sdmx/eval/run_eval.py``); this test only
    locks in the LRU contract.
    """
    load_calls: list[str] = []

    async def _spy_from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        load_calls.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(search_module.Catalog, "from_url", _spy_from_url)

    df1 = asyncio.run(
        sdmx_series_search(
            SeriesSearchParams(query="HICP", flow_id="ECB/test", limit=1)
        )
    )
    df2 = asyncio.run(
        sdmx_series_search(
            SeriesSearchParams(query="HICP again", flow_id="ECB/test", limit=1)
        )
    )

    assert len(load_calls) == 1
    col_names = [c.name for c in df1.columns]
    assert col_names == ["series_key", "title", "similarity", "namespace"]
    assert df1.df["series_key"].iloc[0] == "A.1"
    assert df2.df["series_key"].iloc[0] == "A.1"


def test_empty_search_results_raise_empty_data_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero matches raise ``EmptyDataError`` with a recovery directive —
    silent empty list would mislead the agent."""

    class _EmptyCatalog:
        async def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            return []

    async def _from_url(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        return _EmptyCatalog()

    monkeypatch.setattr(search_module.Catalog, "from_url", _from_url)

    with pytest.raises(EmptyDataError, match="No matches"):
        asyncio.run(
            sdmx_series_search(
                SeriesSearchParams(
                    query="nonsense", flow_id="ECB/test", limit=1
                )
            )
        )


# ---------------------------------------------------------------------------
# Kernel-error → ConnectorError wrapping
# ---------------------------------------------------------------------------


def test_repo_not_found_wraps_into_connector_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``RepositoryNotFoundError`` from the kernel is wrapped with a
    recovery directive so the agent has a path forward."""

    async def _raise_repo_not_found(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        raise RepositoryNotFoundError("404 not found")

    monkeypatch.setattr(search_module.Catalog, "from_url", _raise_repo_not_found)

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        asyncio.run(
            sdmx_series_search(
                SeriesSearchParams(
                    query="x", flow_id="ECB/never_published", limit=1
                )
            )
        )


def test_missing_bundle_wraps_into_connector_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``FileNotFoundError`` (kernel ``_load_file`` couldn't find
    ``meta.json``) becomes a directive-bearing ``ConnectorError``."""

    async def _raise_file_not_found(url: str, *, embedder: Any = None) -> Any:  # noqa: ARG001
        raise FileNotFoundError("meta.json not found")

    monkeypatch.setattr(search_module.Catalog, "from_url", _raise_file_not_found)

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        asyncio.run(
            sdmx_series_search(
                SeriesSearchParams(query="x", flow_id="ECB/test", limit=1)
            )
        )
