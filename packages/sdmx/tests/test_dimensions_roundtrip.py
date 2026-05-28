"""SDMX dimensions flatten cleanly into Parsimony catalog metadata."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from parsimony.result import Column, ColumnRole, OutputConfig

from parsimony_sdmx.connectors.enumerate_series import ENUMERATE_SERIES_OUTPUT, _series_frame
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord
from parsimony_sdmx.io.parquet import SERIES_SCHEMA, write_series


def test_write_series_persists_dimensions(tmp_path: Path) -> None:
    rows = [
        SeriesRecord(
            id="A.U2",
            dataset_id="YC",
            title="Annual - Euro area",
            dimensions=(
                DimensionValue(id="FREQ", code="A", label="Annual"),
                DimensionValue(id="REF_AREA", code="U2", label="Euro area"),
            ),
        ),
        SeriesRecord(
            id="M.U2",
            dataset_id="YC",
            title="Monthly - Euro area",
            dimensions=(
                DimensionValue(id="FREQ", code="M", label="Monthly"),
                DimensionValue(id="REF_AREA", code="U2", label="Euro area"),
            ),
        ),
    ]
    path = write_series(rows, tmp_path, "ECB", "YC")
    table = pq.read_table(path)

    assert table.schema == SERIES_SCHEMA
    assert table.column("dimensions").to_pylist() == [
        [
            {"id": "FREQ", "code": "A", "label": "Annual"},
            {"id": "REF_AREA", "code": "U2", "label": "Euro area"},
        ],
        [
            {"id": "FREQ", "code": "M", "label": "Monthly"},
            {"id": "REF_AREA", "code": "U2", "label": "Euro area"},
        ],
    ]


def test_write_series_accepts_empty_dimensions(tmp_path: Path) -> None:
    rows = [SeriesRecord(id="S1", dataset_id="YC", title="t")]
    path = write_series(rows, tmp_path, "ECB", "YC")
    table = pq.read_table(path)

    assert table.column("dimensions").to_pylist() == [[]]


def test_series_frame_flattens_dimensions_to_metadata_columns() -> None:
    frame = _series_frame(
        [
            SeriesRecord(
                id="A.U2",
                dataset_id="YC",
                title="Annual - Euro area",
                dimensions=(
                    DimensionValue(id="FREQ", code="A", label="Annual"),
                    DimensionValue(id="REF_AREA", code="U2", label="Euro area"),
                ),
            ),
            SeriesRecord(
                id="D.X",
                dataset_id="YC",
                title="Daily - X",
                dimensions=(
                    DimensionValue(id="FREQ", code="D", label="Daily"),
                    DimensionValue(id="REF_AREA", code="X"),
                ),
            ),
        ],
        agency="ECB",
        dataset_id="YC",
    )

    assert list(frame["code"]) == ["A.U2", "D.X"]
    assert list(frame["FREQ_code"]) == ["A", "D"]
    assert list(frame["FREQ_label"]) == ["Annual", "Daily"]
    assert list(frame["REF_AREA_code"]) == ["U2", "X"]
    assert pd.isna(frame.loc[1, "REF_AREA_label"])


def test_entries_from_sdmx_result_receives_dimension_metadata() -> None:
    df = pd.DataFrame(
        {
            "code": ["A.U2", "D.X"],
            "title": ["Annual euro area", "Daily unknown"],
            "FREQ_code": ["A", "D"],
            "FREQ_label": ["Annual", "Daily"],
            "REF_AREA_code": ["U2", "X"],
            "REF_AREA_label": ["Euro area", None],
            "agency": ["ECB", "ECB"],
            "dataset_id": ["YC", "YC"],
        }
    )
    schema = OutputConfig(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace="sdmx_series_ecb_yc"),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="FREQ_code", role=ColumnRole.METADATA),
            Column(name="FREQ_label", role=ColumnRole.METADATA),
            Column(name="REF_AREA_code", role=ColumnRole.METADATA),
            Column(name="REF_AREA_label", role=ColumnRole.METADATA),
            Column(name="agency", role=ColumnRole.METADATA),
            Column(name="dataset_id", role=ColumnRole.METADATA),
        ]
    )
    entries = schema.build_entities(df)

    by_code = {entry.code: entry for entry in entries}
    assert by_code["A.U2"].metadata["FREQ_code"] == "A"
    assert by_code["A.U2"].metadata["FREQ_label"] == "Annual"
    assert by_code["A.U2"].metadata["REF_AREA_code"] == "U2"
    assert by_code["A.U2"].metadata["REF_AREA_label"] == "Euro area"
    assert by_code["D.X"].metadata["REF_AREA_code"] == "X"
    assert "REF_AREA_label" not in by_code["D.X"].metadata


def test_enumerator_output_uses_wildcard_metadata_columns() -> None:
    roles = {column.name: column.role.value for column in ENUMERATE_SERIES_OUTPUT.columns}
    assert roles == {
        "code": "key",
        "title": "title",
        "*": "metadata",
    }
