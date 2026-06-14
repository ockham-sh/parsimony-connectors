"""Tests for scoped ``enumerate_sdmx_series`` discovery."""

from __future__ import annotations

import pytest
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import (
    MAX_DISCOVERY_RESULTS,
    EnumerateSeriesParams,
    enumerate_sdmx_series,
)
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord

_REGISTRY = "parsimony_sdmx.providers.registry.get_provider"


def _stub_records() -> list[SeriesRecord]:
    return [
        SeriesRecord(
            id="M..TOTAL.PC_ACT.T.DE",
            dataset_id="UNE_RT_M",
            title="Monthly unemployment rate, Germany",
            dimensions=(
                DimensionValue(id="freq", code="M", label="Monthly"),
                DimensionValue(id="geo", code="DE", label="Germany"),
                DimensionValue(id="unit", code="PC_ACT", label="Percentage"),
            ),
        ),
    ]


@pytest.fixture
def mock_discover(monkeypatch: pytest.MonkeyPatch):
    def _fake(dataset_id: str, partial_key: str) -> list[SeriesRecord]:
        return _stub_records()

    class _Provider:
        agency_id = "ESTAT"

        def discover_series_keys(self, dataset_id: str, partial_key: str) -> list[SeriesRecord]:
            return _fake(dataset_id, partial_key)

    monkeypatch.setattr(_REGISTRY, lambda agency_id: _Provider())


def test_scoped_discovery_returns_labeled_rows(mock_discover) -> None:
    df = enumerate_sdmx_series(agency=AgencyId.ESTAT, dataset_id="UNE_RT_M", key_pattern="M....DE").data
    assert df.iloc[0]["code"] == "M..TOTAL.PC_ACT.T.DE"
    assert df.iloc[0]["freq_label"] == "Monthly"
    assert df.iloc[0]["geo_code"] == "DE"


def test_empty_discovery_raises_emptydata(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Provider:
        def discover_series_keys(self, dataset_id: str, partial_key: str) -> list[SeriesRecord]:
            return []

    monkeypatch.setattr(_REGISTRY, lambda _: _Provider())
    with pytest.raises(EmptyDataError, match="No series match"):
        enumerate_sdmx_series(agency=AgencyId.ESTAT, dataset_id="UNE_RT_M", key_pattern="M....DE")


def test_over_limit_raises_connector_error_with_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    many = [
        SeriesRecord(
            id=f"S{i}",
            dataset_id="X",
            title=f"Series {i}",
            dimensions=(DimensionValue(id="unit", code=f"C{i}", label=f"Label {i}"),),
        )
        for i in range(MAX_DISCOVERY_RESULTS + 1)
    ]

    class _Provider:
        def discover_series_keys(self, dataset_id: str, partial_key: str) -> list[SeriesRecord]:
            return many

    monkeypatch.setattr(_REGISTRY, lambda _: _Provider())
    with pytest.raises(ConnectorError, match="Pin more dimensions"):
        enumerate_sdmx_series(agency=AgencyId.ESTAT, dataset_id="X", key_pattern="....")


def test_invalid_parameter_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Provider:
        def discover_series_keys(self, dataset_id: str, partial_key: str) -> list[SeriesRecord]:
            raise InvalidParameterError("sdmx", "bad key position count")

    monkeypatch.setattr(_REGISTRY, lambda _: _Provider())
    with pytest.raises(InvalidParameterError):
        enumerate_sdmx_series(agency=AgencyId.ESTAT, dataset_id="X", key_pattern="bad")


def test_accepts_lowercase_agency() -> None:
    params = EnumerateSeriesParams(agency="ecb", dataset_id="YC", key_pattern="M....")  # type: ignore[arg-type]
    assert params.agency is AgencyId.ECB
