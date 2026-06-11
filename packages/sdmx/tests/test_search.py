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

from collections.abc import Iterator
from typing import Any

import pytest
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError

from parsimony_sdmx.connectors import search as search_module
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.search import (
    DEFAULT_CATALOG_ROOT,
    PARSIMONY_SDMX_CATALOG_URL_ENV,
    _clear_catalog_lru,
    _resolve_datasets_namespace,
    _resolve_series_namespace,
    sdmx_datasets_search,
    sdmx_series_search,
)

_LOAD_OR_BUILD = "parsimony.catalog.search.load_or_build_catalog"
_CATALOG_LOAD = "parsimony.catalog.Catalog.load"


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

    def test_bare_token_without_agency_raises(self) -> None:
        with pytest.raises(ConnectorError, match="must include agency"):
            _resolve_series_namespace("HICP")


class TestResolveDatasetsNamespace:
    def test_explicit_agency(self) -> None:
        assert _resolve_datasets_namespace(agency=AgencyId.ECB) == "sdmx_datasets_ecb"

    def test_string_agency(self) -> None:
        assert _resolve_datasets_namespace(agency="ECB") == "sdmx_datasets_ecb"

    def test_missing_agency_raises(self) -> None:
        with pytest.raises(ConnectorError, match="requires agency"):
            _resolve_datasets_namespace(agency=None)


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

    def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
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

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1)

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/sdmx_series_ecb_test"]


def test_env_overrides_default_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """``PARSIMONY_SDMX_CATALOG_URL`` overrides the default — useful for
    pointing at a local snapshot during catalog dev."""
    monkeypatch.setenv(PARSIMONY_SDMX_CATALOG_URL_ENV, "file:///tmp/local-sdmx/")
    seen: list[str] = []

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1)

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

    def _spy_load(url: str) -> Any:
        load_calls.append(url)
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    df1 = sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1)
    df2 = sdmx_series_search(query="HICP again", flow_id="ECB/test", limit=1)

    assert len(load_calls) == 1
    col_names = list(df1.data.columns)
    assert col_names == ["series_key", "title", "score", "namespace"]
    assert df1.data["series_key"].iloc[0] == "A.1"
    assert df2.data["series_key"].iloc[0] == "A.1"


def test_series_search_uses_loaded_catalog_default_ranker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Series search relies on the serialized catalog default ranker."""

    seen: dict[str, Any] = {}

    class _CatalogWithDefaultRanker(_FakeCatalog):
        def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            seen["called"] = True
            return super().search(query, limit, namespaces=namespaces)

    def _spy_load(url: str) -> Any:
        return _CatalogWithDefaultRanker()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    sdmx_series_search(query="HICP", flow_id="ECB/test", limit=1)

    assert seen.get("called") is True


def test_empty_search_results_raise_empty_data_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero matches raise ``EmptyDataError`` with a recovery directive —
    silent empty list would mislead the agent."""

    class _EmptyCatalog:
        indexes: list[Any] = []

        def search(self, query, limit, *, namespaces=None):  # noqa: ARG002
            return [], []

    def _spy_load(url: str) -> Any:
        return _EmptyCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    with pytest.raises(EmptyDataError, match="No matches"):
        sdmx_series_search(query="nonsense", flow_id="ECB/test", limit=1)


# ---------------------------------------------------------------------------
# Kernel-error → ConnectorError wrapping
# ---------------------------------------------------------------------------


def test_repo_not_found_wraps_into_connector_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CatalogLRU ``ConnectorError`` for a missing repo is wrapped with a
    recovery directive so the agent has a path forward."""

    def _raise_catalog_not_found(url: str, *, cache_path: Any = None, build: Any = None) -> Any:
        raise ConnectorError(f"Catalog repo not found at {url}. DO NOT retry.", provider="catalog")

    monkeypatch.setattr(search_module._lru, "get_or_load", _raise_catalog_not_found)
    search_module._clear_catalog_lru()

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        sdmx_series_search(query="x", flow_id="ECB/never_published", limit=1)


def test_missing_bundle_wraps_into_connector_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``FileNotFoundError`` (kernel ``_load_file`` couldn't find
    ``meta.json``) becomes a directive-bearing ``ConnectorError``."""

    def _raise_missing(url: str, *, cache_path: Any, build: Any = None) -> Any:
        raise ConnectorError(f"Catalog bundle not present at {url}. DO NOT retry.", provider="catalog")

    monkeypatch.setattr(_LOAD_OR_BUILD, _raise_missing)
    search_module._clear_catalog_lru()

    with pytest.raises(ConnectorError, match="DO NOT retry"):
        sdmx_series_search(query="x", flow_id="ECB/test", limit=1)


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

    with pytest.raises(InvalidParameterError, match=">= 1"):
        set_catalog_lru_size(0)


def test_datasets_search_code_query_loads_agency_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog(
            code="ECB|YC",
            title="Yield curve",
            metadata={"agency": "ECB", "dataset_id": "YC", "dimensions": []},
        )

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    sdmx_datasets_search(query="code: ECB|YC", agency="ECB", limit=1)

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/sdmx_datasets_ecb"]


def test_datasets_search_explicit_agency_loads_agency_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def _spy_load(url: str) -> Any:
        seen.append(url)
        return _FakeCatalog(
            code="ECB|YC",
            title="Yield curve",
            metadata={"agency": "ECB", "dataset_id": "YC", "dimensions": []},
        )

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    sdmx_datasets_search(query="yield curve", agency="ECB", limit=1)

    assert seen == [f"{DEFAULT_CATALOG_ROOT}/sdmx_datasets_ecb"]


def test_datasets_search_empty_agency_raises() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        sdmx_datasets_search(query="yield curve", agency="", limit=1)


def test_datasets_search_returns_dimensions_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = [{"id": "FREQ", "values": [{"code": "M", "label": "Monthly"}]}]

    def _spy_load(url: str) -> Any:
        if url.endswith("/sdmx_datasets_ecb"):
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

    df = sdmx_datasets_search(query="code: ECB|YC", agency="ECB", limit=1)

    assert df.data["flow_id"].iloc[0] == "ECB/YC"
    assert df.data["dimensions"].iloc[0] == manifest


def test_datasets_search_returns_empty_dimensions_when_manifest_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _spy_load(url: str) -> Any:
        if url.endswith("/sdmx_datasets_ecb"):
            return _FakeCatalog(
                code="ECB|YC",
                title="Yield curve",
                metadata={"agency": "ECB", "dataset_id": "YC"},
            )
        return _FakeCatalog()

    monkeypatch.setattr(_CATALOG_LOAD, _spy_load)
    search_module._clear_catalog_lru()

    df = sdmx_datasets_search(query="code: ECB|YC", agency="ECB", limit=1)

    assert df.data["dimensions"].iloc[0] == []
