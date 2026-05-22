"""Tests for ``enumerate_sdmx_series`` — live per-dataset enumerator.

Mocks :func:`parsimony_sdmx._isolation.fetch_series` directly — the
real subprocess primitive (spawn, timeout, parquet roundtrip) is
covered by ``test_worker.py`` / ``test_listing.py``. Here we just
exercise the enumerator's contract: validation, shaping, empty-data,
subprocess-error passthrough.
"""

from __future__ import annotations

import pytest

from parsimony_sdmx._isolation import FetchSeriesError
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import (
    EnumerateSeriesParams,
    enumerate_sdmx_series,
    series_namespace,
)
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord
from parsimony_sdmx.core.outcomes import DatasetOutcome, OutcomeStatus


def _stub_records() -> list[SeriesRecord]:
    return [
        SeriesRecord(
            id="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
            dataset_id="YC",
            title="10y yield",
            dimensions=(
                DimensionValue(id="FREQ", code="B", label="Daily - businessweek"),
                DimensionValue(id="REF_AREA", code="U2", label="Euro area"),
                DimensionValue(id="DATA_TYPE_FM", code="SR_10Y", label="Yield curve spot rate, 10-year maturity"),
            ),
        ),
        SeriesRecord(
            id="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
            dataset_id="YC",
            title="2y yield",
            dimensions=(
                DimensionValue(id="FREQ", code="B", label="Daily - businessweek"),
                DimensionValue(id="REF_AREA", code="U2", label="Euro area"),
                DimensionValue(id="DATA_TYPE_FM", code="SR_2Y", label="Yield curve spot rate, 2-year maturity"),
            ),
        ),
    ]


@pytest.fixture
def mock_fetch_series(monkeypatch: pytest.MonkeyPatch):
    """Replace ``fetch_series`` with an inline stub returning canned records."""

    def _fake(agency_id: str, dataset_id: str, timeout_s: float = 0.0):
        return _stub_records()

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_series.fetch_series",
        _fake,
    )
    return _fake


@pytest.mark.asyncio
async def test_enumerates_one_dataset(mock_fetch_series) -> None:
    result = await enumerate_sdmx_series(agency=AgencyId.ECB, dataset_id="YC")
    entries = result.data
    by_code = {entry.code: entry for entry in entries}
    assert set(by_code) == {
        "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
        "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    }
    metadata_codes = {entry.metadata["DATA_TYPE_FM_code"] for entry in entries}
    assert metadata_codes == {"SR_10Y", "SR_2Y"}
    metadata_labels = {entry.metadata["DATA_TYPE_FM_label"] for entry in entries}
    assert "Yield curve spot rate, 10-year maturity" in metadata_labels
    assert {entry.metadata["agency"] for entry in entries} == {"ECB"}
    assert {entry.metadata["dataset_id"] for entry in entries} == {"YC"}


@pytest.mark.asyncio
async def test_empty_live_response_raises_emptydata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from parsimony.errors import EmptyDataError

    def _fake(agency_id: str, dataset_id: str, timeout_s: float = 0.0):
        return []

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_series.fetch_series",
        _fake,
    )

    with pytest.raises(EmptyDataError, match="zero series"):
        await enumerate_sdmx_series(agency=AgencyId.ECB, dataset_id="EMPTY")


@pytest.mark.asyncio
async def test_accepts_lowercase_agency_from_namespace_parsing() -> None:
    """Build-script namespace parsing may pass ``agency="ecb"``."""
    params = EnumerateSeriesParams(agency="ecb", dataset_id="YC")  # type: ignore[arg-type]
    assert params.agency is AgencyId.ECB


@pytest.mark.asyncio
async def test_subprocess_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A FetchSeriesError from the subprocess must surface to the caller.

    The kernel publisher catches per-namespace failures and reports a
    failed bundle; this test just pins the wrapper contract.
    """

    def _raise(agency_id: str, dataset_id: str, timeout_s: float = 0.0):
        raise FetchSeriesError(
            DatasetOutcome(
                dataset_id=dataset_id,
                agency_id=agency_id,
                status=OutcomeStatus.FAILED,
                error_message="fake failure",
            )
        )

    monkeypatch.setattr(
        "parsimony_sdmx.connectors.enumerate_series.fetch_series",
        _raise,
    )
    with pytest.raises(FetchSeriesError, match="fake failure"):
        await enumerate_sdmx_series(agency=AgencyId.ECB, dataset_id="YC")


def test_series_namespace_lowercases_agency_and_dataset() -> None:
    assert series_namespace(AgencyId.ECB, "YC") == "sdmx_series_ecb_yc"
    assert series_namespace(AgencyId.IMF_DATA, "PGI") == "sdmx_series_imf_data_pgi"


def test_enumerator_output_has_no_namespace_on_key() -> None:
    """Per-dataset namespace comes from the catalog name at ingest time;
    the declared OutputConfig's KEY column must stay namespace-less.
    """
    from parsimony_sdmx.connectors.enumerate_series import ENUMERATE_SERIES_OUTPUT

    key_col = next(c for c in ENUMERATE_SERIES_OUTPUT.columns if c.role.value == "key")
    assert key_col.namespace is None
