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
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.core.outcomes import DatasetOutcome, OutcomeStatus


def _stub_records() -> list[SeriesRecord]:
    return [
        SeriesRecord(
            id="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
            dataset_id="YC",
            title="10y yield",
        ),
        SeriesRecord(
            id="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
            dataset_id="YC",
            title="2y yield",
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
    result = await enumerate_sdmx_series(
        EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="YC"),
    )
    df = result.data
    assert set(df["code"]) == {
        "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
        "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    }
    assert set(df["agency"]) == {"ECB"}
    assert set(df["dataset_id"]) == {"YC"}


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
        await enumerate_sdmx_series(
            EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="EMPTY"),
        )


@pytest.mark.asyncio
async def test_accepts_lowercase_agency_from_resolve_catalog() -> None:
    """When ``RESOLVE_CATALOG`` parses ``sdmx_series_ecb_yc`` it passes
    ``agency="ecb"`` (lowercase). The Pydantic ``before`` validator
    upcases it back to the canonical ``AgencyId.ECB``.
    """
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
        await enumerate_sdmx_series(
            EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="YC"),
        )


def test_series_namespace_lowercases_agency_and_dataset() -> None:
    assert series_namespace(AgencyId.ECB, "YC") == "sdmx_series_ecb_yc"
    assert series_namespace(AgencyId.IMF_DATA, "PGI") == "sdmx_series_imf_data_pgi"


def test_enumerator_output_has_no_namespace_on_key() -> None:
    """Per-dataset namespace comes from the catalog name at ingest time;
    the enumerator's KEY column must stay namespace-less.
    """
    output_config = enumerate_sdmx_series.output_config
    assert output_config is not None
    cols = output_config.columns
    key_col = next(c for c in cols if c.role.value == "key")
    assert key_col.namespace is None
