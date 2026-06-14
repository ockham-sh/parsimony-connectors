"""Pure-parser tests for the BLS flat-file layer (no network)."""

from __future__ import annotations

import pytest
from parsimony.errors import InvalidParameterError

from parsimony_bls import flatfiles as ff

_ROOT_LISTING = (
    "<pre><A HREF=\"/pub/\">[To Parent Directory]</A><br><br>"
    ' 5/12/2026  8:30 AM        &lt;dir&gt; <A HREF="/pub/time.series/cu/">cu</A><br>'
    ' 6/5/2026  8:30 AM        &lt;dir&gt; <A HREF="/pub/time.series/ce/">ce</A><br>'
    ' 2/27/2024 10:13 AM        &lt;dir&gt; <A HREF="/pub/time.series/compressed/">compressed</A><br>'
    "</pre>"
)

_SURVEY_LISTING = (
    "<pre>"
    ' 6/5/2026  8:30 AM      1339447 <A HREF="/pub/time.series/cu/cu.series">cu.series</A><br>'
    ' 6/5/2026  8:30 AM        12644 <A HREF="/pub/time.series/cu/cu.area">cu.area</A><br>'
    ' 6/5/2026  8:30 AM    348357219 <A HREF="/pub/time.series/cu/cu.data.0.Current">cu.data.0.Current</A><br>'
    "</pre>"
)

_SERIES_TSV = (
    "series_id\tarea_code\titem_code\tseasonal\tseries_title\tbegin_year\tend_year\n"
    "CUUR0000SA0 \t0000\tSA0\tU\tAll items in U.S. city average\t1913\t2026\n"
    "CUUR0000SETB01\t0000\tSETB01\tU\tGasoline (all types) in U.S. city average\t1976\t2026\n"
)

_AREA_TSV = "area_code\tarea_name\n0000\tU.S. city average\n0100\tNortheast urban\n"


def test_parse_listing_marks_dirs_and_sizes() -> None:
    rows = ff.parse_listing(_ROOT_LISTING)
    assert ("cu", -1) in rows
    assert ("ce", -1) in rows


def test_parse_listing_reads_file_sizes() -> None:
    rows = dict(ff.parse_listing(_SURVEY_LISTING))
    assert rows["cu.series"] == 1339447
    assert rows["cu.area"] == 12644


def test_parse_tsv_strips_cells_and_header() -> None:
    cols, rows = ff.parse_tsv(_SERIES_TSV)
    assert cols[0] == "series_id"
    assert rows[0]["series_id"] == "CUUR0000SA0"  # trailing space stripped
    assert rows[1]["item_code"] == "SETB01"
    assert len(rows) == 2


def test_dimension_columns_excludes_structural() -> None:
    cols, _ = ff.parse_tsv(_SERIES_TSV)
    assert ff.dimension_columns(cols) == ["area_code", "item_code", "seasonal"]


def test_build_label_map_and_resolve() -> None:
    cols, rows = ff.parse_tsv(_AREA_TSV)
    label_map = ff.build_label_map(cols, rows)
    assert label_map["0000"] == "U.S. city average"
    tables = {"area": label_map}
    assert ff.resolve_label(tables, "area_code", "0000") == "U.S. city average"
    # unknown code falls back to the raw code, not an empty string
    assert ff.resolve_label(tables, "area_code", "9999") == "9999"
    # missing table -> raw code
    assert ff.resolve_label({}, "item_code", "SA0") == "SA0"


def test_dimension_manifest_shape() -> None:
    cols, rows = ff.parse_tsv(_SERIES_TSV)
    tables = {"area": {"0000": "U.S. city average"}, "item": {"SA0": "All items"}}
    manifest = ff.dimension_manifest(cols, rows, tables, max_values=5)
    by_id = {d["id"]: d for d in manifest}
    assert set(by_id) == {"area_code", "item_code", "seasonal"}
    assert by_id["area_code"]["values"] == [{"code": "0000", "label": "U.S. city average"}]


def test_fetch_series_rows_refuses_oversized_file(monkeypatch) -> None:
    def fake_size(session, survey):
        return ff.MAX_SERIES_FILE_BYTES + 1

    from curl_cffi.requests import Session

    monkeypatch.setattr(ff, "series_file_size", fake_size)
    with pytest.raises(InvalidParameterError, match="too large to index"):
        ff.fetch_series_rows(Session(), "CA")  # type: ignore[arg-type]


def test_fetch_series_rows_errors_when_absent(monkeypatch) -> None:
    def fake_size(session, survey):
        return -1

    from curl_cffi.requests import Session

    monkeypatch.setattr(ff, "series_file_size", fake_size)
    with pytest.raises(InvalidParameterError, match="no .series"):
        ff.fetch_series_rows(Session(), "ZZ")  # type: ignore[arg-type]
