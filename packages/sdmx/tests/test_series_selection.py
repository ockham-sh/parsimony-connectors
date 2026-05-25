"""Tests for SDMX series catalog selection heuristics."""

from __future__ import annotations

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.series_selection import select_series_records, should_build_series_catalog


def _record(agency: str, dataset_id: str, title: str) -> DatasetRecord:
    return DatasetRecord(agency_id=agency, dataset_id=dataset_id, title=title)


def test_ecb_builds_all_non_derived_flows() -> None:
    records = [
        _record("ECB", "YC", "Yield curve"),
        _record("ECB", "EXR", "Exchange rates"),
        _record("ECB", "YC$DV", "Derived view"),
    ]
    selected = select_series_records(AgencyId.ECB, records)
    assert {r.dataset_id for r in selected} == {"YC", "EXR"}


def test_estat_filters_non_macro_titles() -> None:
    records = [
        _record("ESTAT", "PRC_HICP_MANR", "HICP - monthly index"),
        _record("ESTAT", "TOUR_OCC", "Tourism occupancy in hotels"),
        _record("ESTAT", "UNE_RT_M", "Unemployment rate"),
    ]
    selected = select_series_records(AgencyId.ESTAT, records)
    ids = {r.dataset_id for r in selected}
    assert "PRC_HICP_MANR" in ids
    assert "UNE_RT_M" in ids
    assert "TOUR_OCC" not in ids


def test_small_agency_builds_all() -> None:
    records = [_record("WB_WDI", f"DS{i}", f"Dataset {i}") for i in range(10)]
    assert len(select_series_records(AgencyId.WB_WDI, records)) == 10


def test_should_build_series_respects_derived_marker() -> None:
    record = _record("ECB", "YC$DV", "Derived")
    assert not should_build_series_catalog(AgencyId.ECB, record, total_flows=1)
