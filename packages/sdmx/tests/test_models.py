from dataclasses import FrozenInstanceError

import pytest

from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord


def test_dataset_record_fields() -> None:
    r = DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield Curve")
    assert r.dataset_id == "YC"
    assert r.agency_id == "ECB"
    assert r.title == "Yield Curve"


def test_dataset_record_is_frozen() -> None:
    r = DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield Curve")
    with pytest.raises(FrozenInstanceError):
        r.title = "mutated"  # type: ignore[misc]


def test_dataset_record_uses_slots() -> None:
    r = DatasetRecord(dataset_id="YC", agency_id="ECB", title="t")
    assert not hasattr(r, "__dict__")


def test_series_record_fields() -> None:
    r = SeriesRecord(id="B.U2.EUR", dataset_id="YC", title="t")
    assert r.id == "B.U2.EUR"
    assert r.dataset_id == "YC"
    assert r.title == "t"


def test_series_record_is_frozen() -> None:
    r = SeriesRecord(id="B.U2.EUR", dataset_id="YC", title="t")
    with pytest.raises(FrozenInstanceError):
        r.id = "mutated"  # type: ignore[misc]


def test_records_are_hashable_and_equatable() -> None:
    a = DatasetRecord(dataset_id="YC", agency_id="ECB", title="t")
    b = DatasetRecord(dataset_id="YC", agency_id="ECB", title="t")
    assert a == b
    assert hash(a) == hash(b)
