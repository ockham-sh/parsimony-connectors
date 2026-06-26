"""Index-policy + entity-shaping tests for the BLS catalogs (no network)."""

from __future__ import annotations

from parsimony.catalog import BM25Index, Entity

from parsimony_bls.catalog_policy import (
    discover_dim_codes,
    manifest_from_series_entries,
    series_entries,
    series_indexes,
    surveys_indexes,
)


def _series_entity(code: str, title: str, **meta: str) -> Entity:
    return Entity(namespace="bls_series_cu", code=code, title=title, metadata=meta)


_ENTRIES = [
    _series_entity(
        "CUUR0000SA0",
        "All items in U.S. city average",
        survey="CU",
        area_code="0000",
        area_label="U.S. city average",
        item_code="SA0",
        item_label="All items",
    ),
    _series_entity(
        "CUUR0000SETB01",
        "Gasoline (all types) in U.S. city average",
        survey="CU",
        area_code="0000",
        area_label="U.S. city average",
        item_code="SETB01",
        item_label="Gasoline (all types)",
    ),
]


def test_discover_dim_codes_from_label_keys() -> None:
    assert discover_dim_codes(_ENTRIES) == ["area", "item"]


def test_series_entries_adds_label_valued_dimension_keys() -> None:
    augmented = series_entries(_ENTRIES, ["area", "item"])
    meta = augmented[1].metadata
    assert meta["area"] == "U.S. city average"  # label-valued, for the index
    assert meta["item"] == "Gasoline (all types)"
    assert meta["item_code"] == "SETB01"  # raw code preserved


def test_series_indexes_code_is_bm25_and_dims_present() -> None:
    augmented = series_entries(_ENTRIES, ["area", "item"])
    indexes = series_indexes(augmented, ["area", "item"])
    assert isinstance(indexes["code"], BM25Index)
    assert "title" in indexes
    assert set(["area", "item"]).issubset(indexes)


def test_surveys_indexes_code_is_bm25() -> None:
    survey_entries = [Entity(namespace="bls_surveys", code="CU", title="Consumer Price Index", metadata={})]
    indexes = surveys_indexes(survey_entries)
    assert isinstance(indexes["code"], BM25Index)
    assert "title" in indexes


def test_manifest_from_series_entries() -> None:
    manifest = manifest_from_series_entries(_ENTRIES)
    by_id = {d["id"]: d for d in manifest}
    assert set(by_id) == {"area", "item"}
    assert {"code": "SETB01", "label": "Gasoline (all types)"} in by_id["item"]["values"]
    assert by_id["area"]["values"] == [{"code": "0000", "label": "U.S. city average"}]  # deduped
