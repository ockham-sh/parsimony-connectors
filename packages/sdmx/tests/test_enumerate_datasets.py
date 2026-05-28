"""Tests for ``enumerate_sdmx_datasets`` — live cross-agency listing enumerator.

Mocks :func:`parsimony_sdmx._isolation.list_datasets` directly — the
real subprocess primitive is covered by ``test_listing.py``.
"""

from __future__ import annotations

import pytest
from parsimony.entity import Entity
from parsimony.result import ColumnRole

from parsimony_sdmx._isolation import ListDatasetsError
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import (
    SDMX_DATASETS_ENUM_OUTPUT,
    datasets_namespace,
    enumerate_sdmx_datasets,
    is_datasets_namespace,
    parse_datasets_namespace,
)
from parsimony_sdmx.core.models import DatasetRecord


def _records(agency: str, pairs: list[tuple[str, str]]) -> list[DatasetRecord]:
    return [DatasetRecord(dataset_id=did, agency_id=agency, title=title) for did, title in pairs]


@pytest.fixture
def mock_list_datasets(monkeypatch: pytest.MonkeyPatch):
    """Route ``list_datasets(agency)`` to per-agency canned responses.

    Replaces the subprocess call with an inline dict lookup — the
    enumerator just gets the records back synchronously.
    """
    responses: dict[str, list[DatasetRecord]] = {
        "ECB": _records("ECB", [("YC", "Euro Yield Curve"), ("MIR", "Money Market Rates")]),
        "ESTAT": _records("ESTAT", [("prc_hicp_manr", "HICP annual rate of change")]),
        "IMF_DATA": [],  # empty agency — silently skipped
        "WB_WDI": _records("WB_WDI", [("WDI", "World Development Indicators")]),
    }

    def _fake_list(agency_id: str, timeout_s: float = 0.0) -> list[DatasetRecord]:
        return responses[agency_id]

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_datasets.list_datasets",
        _fake_list,
    )
    return responses


@pytest.mark.parametrize(
    ("agency", "expected"),
    [
        (AgencyId.ECB, "sdmx_datasets_ecb"),
        (AgencyId.ESTAT, "sdmx_datasets_estat"),
        (AgencyId.IMF_DATA, "sdmx_datasets_imf_data"),
        (AgencyId.WB_WDI, "sdmx_datasets_wb_wdi"),
    ],
)
def test_datasets_namespace_normalizes_agency(agency: AgencyId, expected: str) -> None:
    assert datasets_namespace(agency) == expected
    assert datasets_namespace(agency.value) == expected


def test_parse_datasets_namespace_round_trip() -> None:
    for agency in AgencyId:
        ns = datasets_namespace(agency)
        assert parse_datasets_namespace(ns) == agency
        assert is_datasets_namespace(ns)


@pytest.mark.asyncio
async def test_enumerates_all_agencies(mock_list_datasets) -> None:
    result = await enumerate_sdmx_datasets()
    entries: list[Entity] = SDMX_DATASETS_ENUM_OUTPUT.build_entities(result.data)
    assert set(entry.code for entry in entries) == {
        "ECB|YC",
        "ECB|MIR",
        "ESTAT|prc_hicp_manr",
        "WB_WDI|WDI",
    }
    assert {entry.title for entry in entries} == {
        "Euro Yield Curve",
        "Money Market Rates",
        "HICP annual rate of change",
        "World Development Indicators",
    }


@pytest.mark.asyncio
async def test_ingests_into_per_agency_namespaces(mock_list_datasets) -> None:
    result = await enumerate_sdmx_datasets()
    entries: list[Entity] = SDMX_DATASETS_ENUM_OUTPUT.build_entities(result.data)

    assert {entry.namespace for entry in entries} == {
        "sdmx_datasets_ecb",
        "sdmx_datasets_estat",
        "sdmx_datasets_wb_wdi",
    }
    codes_to_titles = {e.code: e.title for e in entries}
    assert codes_to_titles["ECB|YC"] == "Euro Yield Curve"


@pytest.mark.asyncio
async def test_agency_failure_skipped_silently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One dead agency must not sink the whole listing."""

    def _fake_list(agency_id: str, timeout_s: float = 0.0) -> list[DatasetRecord]:
        if agency_id == "ECB":
            return _records("ECB", [("YC", "Euro Yield Curve")])
        raise ListDatasetsError(
            kind="http_error",
            message=f"{agency_id} upstream is down",
            traceback_str="",
        )

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_datasets.list_datasets",
        _fake_list,
    )

    result = await enumerate_sdmx_datasets()
    entries: list[Entity] = SDMX_DATASETS_ENUM_OUTPUT.build_entities(result.data)
    assert [entry.code for entry in entries] == ["ECB|YC"]
    assert entries[0].namespace == "sdmx_datasets_ecb"


@pytest.mark.asyncio
async def test_all_agencies_fail_raises_emptydata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parsimony.errors import EmptyDataError

    def _fake_list(agency_id: str, timeout_s: float = 0.0) -> list[DatasetRecord]:
        raise ListDatasetsError(kind="http_error", message="no network", traceback_str="")

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_datasets.list_datasets",
        _fake_list,
    )

    with pytest.raises(EmptyDataError, match="no rows for any agency"):
        await enumerate_sdmx_datasets()


def test_enumerator_metadata_shape() -> None:
    from parsimony_sdmx.connectors.enumerate_datasets import _datasets_output_config

    cols = _datasets_output_config(AgencyId.ECB).columns
    key_cols = [c for c in cols if c.role == ColumnRole.KEY]
    assert len(key_cols) == 1
    assert key_cols[0].namespace == "sdmx_datasets_ecb"
