"""Tests for SDMX series catalog selection heuristics."""

from __future__ import annotations

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.series_selection import (
    prioritize_series_records,
    select_series_records,
    should_build_series_catalog,
)


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


def test_estat_recall_fix_includes_core_macro_families() -> None:
    """Regression: plural keywords and NACE-labelled industry breakdowns stay macro."""
    must_have = [
        ("NAMA_10_A10", "Gross value added by industry (NACE Rev. 2)"),
        ("NAMQ_10_EXI", "Exports and imports of goods and services"),
        ("IRT_LT_MCBY_M", "Long-term interest rates for convergence purposes"),
        ("STS_INPP_M", "Industrial producer prices index"),
        ("BOP_FDI", "Foreign direct investment"),
        ("EI_BSCI_M_R2", "Business climate indicator"),
    ]
    records = [_record("ESTAT", dataset_id, title) for dataset_id, title in must_have]
    selected = {r.dataset_id for r in select_series_records(AgencyId.ESTAT, records)}
    assert selected == {dataset_id for dataset_id, _ in must_have}


def test_estat_plural_price_keyword_matches() -> None:
    records = [_record("ESTAT", "STS_INPP_M", "Industrial producer prices index")]
    assert select_series_records(AgencyId.ESTAT, records)


def test_estat_prioritization_puts_macro_core_first() -> None:
    records = [
        _record("ESTAT", "TOUR_X", "tail"),
        _record("ESTAT", "NAMA_10_GDP", "gdp"),
        _record("ESTAT", "BOP_C6_M", "bop"),
    ]
    ordered = [r.dataset_id for r in prioritize_series_records(AgencyId.ESTAT, records)]
    assert ordered[0] == "NAMA_10_GDP"
    assert ordered.index("BOP_C6_M") < ordered.index("TOUR_X")


def test_prioritization_preserves_order_for_non_estat() -> None:
    records = [_record("ECB", "YC", "yc"), _record("ECB", "EXR", "exr")]
    assert [r.dataset_id for r in prioritize_series_records(AgencyId.ECB, records)] == ["YC", "EXR"]
