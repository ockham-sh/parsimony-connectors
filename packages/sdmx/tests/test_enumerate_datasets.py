"""Tests for ``enumerate_sdmx_datasets`` — live cross-agency listing enumerator.

Mocks :func:`parsimony_sdmx._isolation.list_datasets` directly — the
real subprocess primitive is covered by ``test_listing.py``.
"""

from __future__ import annotations

import pytest
from parsimony.catalog import entries_from_result

from parsimony_sdmx._isolation import ListDatasetsError
from parsimony_sdmx.connectors.enumerate_datasets import (
    DATASETS_NAMESPACE,
    EnumerateDatasetsParams,
    enumerate_sdmx_datasets,
)
from parsimony_sdmx.core.models import DatasetRecord


def _records(agency: str, pairs: list[tuple[str, str]]) -> list[DatasetRecord]:
    return [
        DatasetRecord(dataset_id=did, agency_id=agency, title=title)
        for did, title in pairs
    ]


@pytest.fixture
def mock_list_datasets(monkeypatch: pytest.MonkeyPatch):
    """Route ``list_datasets(agency)`` to per-agency canned responses.

    Replaces the subprocess call with an inline dict lookup — the
    enumerator just gets the records back synchronously.
    """
    responses: dict[str, list[DatasetRecord]] = {
        "ECB": _records(
            "ECB", [("YC", "Euro Yield Curve"), ("MIR", "Money Market Rates")]
        ),
        "ESTAT": _records(
            "ESTAT", [("prc_hicp_manr", "HICP annual rate of change")]
        ),
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


@pytest.mark.asyncio
async def test_enumerates_all_agencies(mock_list_datasets) -> None:
    result = await enumerate_sdmx_datasets(EnumerateDatasetsParams())
    df = result.data
    assert set(df["code"]) == {
        "ECB|YC",
        "ECB|MIR",
        "ESTAT|prc_hicp_manr",
        "WB_WDI|WDI",
    }
    assert set(df.columns) == {"code", "title", "agency", "dataset_id"}


@pytest.mark.asyncio
async def test_ingests_into_expected_namespace(mock_list_datasets) -> None:
    result = await enumerate_sdmx_datasets(EnumerateDatasetsParams())
    output_config = enumerate_sdmx_datasets.output_config
    assert output_config is not None
    table = result.to_table(output_config)
    entries = entries_from_result(table)

    assert all(e.namespace == DATASETS_NAMESPACE for e in entries)
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

    result = await enumerate_sdmx_datasets(EnumerateDatasetsParams())
    df = result.data
    assert list(df["code"]) == ["ECB|YC"]


@pytest.mark.asyncio
async def test_all_agencies_fail_raises_emptydata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parsimony.errors import EmptyDataError

    def _fake_list(agency_id: str, timeout_s: float = 0.0) -> list[DatasetRecord]:
        raise ListDatasetsError(
            kind="http_error", message="no network", traceback_str=""
        )

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_datasets.list_datasets",
        _fake_list,
    )

    with pytest.raises(EmptyDataError, match="no rows for any agency"):
        await enumerate_sdmx_datasets(EnumerateDatasetsParams())


def test_enumerator_metadata_shape() -> None:
    output_config = enumerate_sdmx_datasets.output_config
    assert output_config is not None
    cols = output_config.columns
    key_cols = [c for c in cols if c.role.value == "key"]
    assert len(key_cols) == 1
    assert key_cols[0].namespace == "sdmx_datasets"
