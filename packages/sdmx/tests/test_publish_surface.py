"""Tests for ``parsimony_sdmx.CATALOGS`` and ``RESOLVE_CATALOG``.

``CATALOGS`` walks live via ``_isolation.list_datasets``, so we mock
that module boundary. ``RESOLVE_CATALOG`` is pure string parsing with
zero I/O — tested straight.
"""

from __future__ import annotations

import pytest

from parsimony_sdmx import CATALOGS, RESOLVE_CATALOG
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import series_namespace
from parsimony_sdmx.core.models import DatasetRecord


async def _collect(gen):
    out = []
    async for item in gen:
        out.append(item)
    return out


@pytest.fixture
def mock_list_datasets(monkeypatch: pytest.MonkeyPatch):
    responses: dict[str, list[DatasetRecord]] = {
        "ECB": [
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="Euro Yield Curve"),
            DatasetRecord(dataset_id="MIR", agency_id="ECB", title="MIR"),
        ],
        "ESTAT": [
            DatasetRecord(
                dataset_id="prc_hicp_manr",
                agency_id="ESTAT",
                title="HICP annual rate",
            ),
        ],
        "IMF_DATA": [],
        "WB_WDI": [],
    }

    def _fake_list(agency_id: str, timeout_s: float = 0.0) -> list[DatasetRecord]:
        return responses[agency_id]

    # CATALOGS imports ``list_datasets`` lazily from parsimony_sdmx._isolation
    # inside the generator body. Patching the attribute on the package makes
    # the lazy import pick up our fake.
    monkeypatch.setattr(
        "parsimony_sdmx._isolation.list_datasets",
        _fake_list,
    )
    return responses


# ---------------------------------------------------------------------------
# CATALOGS async generator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalogs_yields_static_datasets_first(mock_list_datasets) -> None:
    entries = await _collect(CATALOGS())
    assert entries[0][0] == "sdmx_datasets"
    assert callable(entries[0][1])


@pytest.mark.asyncio
async def test_catalogs_fans_out_to_one_per_dataset(mock_list_datasets) -> None:
    entries = await _collect(CATALOGS())
    namespaces = {ns for ns, _ in entries}
    assert namespaces == {
        "sdmx_datasets",
        "sdmx_series_ecb_yc",
        "sdmx_series_ecb_mir",
        "sdmx_series_estat_prc_hicp_manr",
    }


@pytest.mark.asyncio
async def test_catalogs_skips_agencies_that_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parsimony_sdmx._isolation import ListDatasetsError

    def _fake_list(agency_id: str, timeout_s: float = 0.0) -> list[DatasetRecord]:
        if agency_id == "ECB":
            return [DatasetRecord(dataset_id="YC", agency_id="ECB", title="YC")]
        raise ListDatasetsError(
            kind="http_error",
            message=f"{agency_id} down",
            traceback_str="",
        )

    monkeypatch.setattr(
        "parsimony_sdmx._isolation.list_datasets",
        _fake_list,
    )

    entries = await _collect(CATALOGS())
    namespaces = {ns for ns, _ in entries}
    assert namespaces == {"sdmx_datasets", "sdmx_series_ecb_yc"}


# ---------------------------------------------------------------------------
# RESOLVE_CATALOG — pure string parsing, no I/O / no SDMX calls
# ---------------------------------------------------------------------------


def test_resolve_catalog_returns_static_datasets_callable() -> None:
    fn = RESOLVE_CATALOG("sdmx_datasets")
    assert callable(fn)
    assert fn is not None
    assert "datasets" in fn.__name__


def test_resolve_catalog_parses_simple_agency_namespace() -> None:
    fn = RESOLVE_CATALOG("sdmx_series_ecb_yc")
    assert callable(fn)
    assert fn is not None
    assert "ECB" in fn.__name__
    # Namespace tail is lowercase; RESOLVE_CATALOG upcases it back to the
    # upstream canonical form (every SDMX agency we wire today uses upper).
    assert "YC" in fn.__name__


def test_resolve_catalog_parses_multi_token_agency_namespace() -> None:
    """Longest-agency match: ``imf_data_pgi`` → ``IMF_DATA`` + ``PGI``."""
    fn = RESOLVE_CATALOG("sdmx_series_imf_data_pgi")
    assert fn is not None
    assert "IMF_DATA" in fn.__name__
    assert "PGI" in fn.__name__


def test_resolve_catalog_upcases_estat_multi_token_tail() -> None:
    """ESTAT uses uppercase dataflow IDs like ``PRC_HICP_MANR``."""
    fn = RESOLVE_CATALOG("sdmx_series_estat_prc_hicp_manr")
    assert fn is not None
    assert "ESTAT" in fn.__name__
    assert "PRC_HICP_MANR" in fn.__name__


def test_resolve_catalog_rejects_unknown_agency() -> None:
    assert RESOLVE_CATALOG("sdmx_series_unknown_yc") is None


def test_resolve_catalog_rejects_missing_dataset_tail() -> None:
    assert RESOLVE_CATALOG("sdmx_series_ecb") is None


def test_resolve_catalog_returns_none_for_foreign_namespace() -> None:
    assert RESOLVE_CATALOG("fred") is None
    assert RESOLVE_CATALOG("some_other_plugin_namespace") is None


def test_series_namespace_helper_matches_resolver_parse() -> None:
    """Round-trip: composing via :func:`series_namespace` must parse back."""
    ns = series_namespace(AgencyId.ECB, "YC")
    assert RESOLVE_CATALOG(ns) is not None
