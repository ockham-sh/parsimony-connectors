from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord
from parsimony_sdmx.io.parquet import (
    DATASETS_SCHEMA,
    SERIES_SCHEMA,
    TMP_SUBDIR,
    write_datasets,
    write_series,
)


class TestWriteDatasets:
    def test_happy_path_round_trip(self, tmp_path: Path) -> None:
        rows = [
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="Yield Curve"),
            DatasetRecord(dataset_id="CPI", agency_id="ECB", title="Consumer Prices"),
        ]
        path = write_datasets(rows, tmp_path, "ECB")
        assert path == tmp_path / "ECB" / "datasets.parquet"
        assert path.exists()

        table = pq.read_table(path)
        assert table.schema == DATASETS_SCHEMA
        assert table.num_rows == 2
        assert set(table.column("dataset_id").to_pylist()) == {"YC", "CPI"}

    def test_schema_is_non_nullable_strings(self, tmp_path: Path) -> None:
        rows = [DatasetRecord(dataset_id="YC", agency_id="ECB", title="t")]
        path = write_datasets(rows, tmp_path, "ECB")
        table = pq.read_table(path)
        for field in table.schema:
            assert field.type == "string"
            assert field.nullable is False

    def test_duplicate_agency_dataset_raises(self, tmp_path: Path) -> None:
        rows = [
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="t1"),
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="t2"),
        ]
        with pytest.raises(ValueError, match="Duplicate"):
            write_datasets(rows, tmp_path, "ECB")

    def test_same_dataset_id_across_agencies_allowed(self, tmp_path: Path) -> None:
        rows = [
            DatasetRecord(dataset_id="CPI", agency_id="ECB", title="ECB CPI"),
            DatasetRecord(dataset_id="CPI", agency_id="IMF", title="IMF CPI"),
        ]
        path = write_datasets(rows, tmp_path, "ECB")
        assert path.exists()

    def test_empty_records_writes_empty_parquet(self, tmp_path: Path) -> None:
        path = write_datasets([], tmp_path, "ECB")
        assert path.exists()
        table = pq.read_table(path)
        assert table.num_rows == 0
        assert table.schema == DATASETS_SCHEMA

    def test_unsafe_agency_id_rejected(self, tmp_path: Path) -> None:
        rows = [DatasetRecord(dataset_id="YC", agency_id="../evil", title="t")]
        with pytest.raises(ValueError):
            write_datasets(rows, tmp_path, "../evil")


class TestWriteSeries:
    def test_happy_path_round_trip(self, tmp_path: Path) -> None:
        rows = [
            SeriesRecord(id="A.B.C", dataset_id="YC", title="Annual - B - C"),
            SeriesRecord(id="D.E.F", dataset_id="YC", title="Daily - E - F"),
        ]
        path = write_series(rows, tmp_path, "ECB", "YC")
        assert path == tmp_path / "ECB" / "series" / "YC.parquet"
        assert path.exists()

        table = pq.read_table(path)
        assert table.schema == SERIES_SCHEMA
        assert table.num_rows == 2
        assert set(table.column("id").to_pylist()) == {"A.B.C", "D.E.F"}

    def test_batching_splits_across_row_groups(self, tmp_path: Path) -> None:
        rows = [
            SeriesRecord(id=f"S{i}", dataset_id="YC", title=f"title {i}")
            for i in range(2500)
        ]
        path = write_series(rows, tmp_path, "ECB", "YC", batch_size=1000)
        table = pq.read_table(path)
        assert table.num_rows == 2500

    def test_streaming_generator_input(self, tmp_path: Path) -> None:
        def gen() -> Iterator[SeriesRecord]:
            for i in range(100):
                yield SeriesRecord(id=f"S{i}", dataset_id="YC", title=f"t{i}")

        path = write_series(gen(), tmp_path, "ECB", "YC", batch_size=25)
        table = pq.read_table(path)
        assert table.num_rows == 100

    def test_duplicate_series_id_raises(self, tmp_path: Path) -> None:
        rows = [
            SeriesRecord(id="S1", dataset_id="YC", title="t1"),
            SeriesRecord(id="S1", dataset_id="YC", title="t2"),
        ]
        with pytest.raises(ValueError, match="Duplicate series id"):
            write_series(rows, tmp_path, "ECB", "YC")

    def test_mismatched_dataset_id_raises(self, tmp_path: Path) -> None:
        rows = [SeriesRecord(id="S1", dataset_id="OTHER", title="t")]
        with pytest.raises(ValueError, match="does not match"):
            write_series(rows, tmp_path, "ECB", "YC")

    def test_empty_records_writes_empty_parquet(self, tmp_path: Path) -> None:
        path = write_series([], tmp_path, "ECB", "YC")
        assert path.exists()
        table = pq.read_table(path)
        assert table.num_rows == 0

    def test_unsafe_dataset_id_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            write_series([], tmp_path, "ECB", "../evil")

    def test_invalid_batch_size_raises(self, tmp_path: Path) -> None:
        rows = [SeriesRecord(id="S1", dataset_id="YC", title="t")]
        with pytest.raises(ValueError, match="batch_size"):
            write_series(rows, tmp_path, "ECB", "YC", batch_size=0)


class TestAtomicWrite:
    def test_no_canonical_file_when_writer_fails(self, tmp_path: Path) -> None:
        rows_bad = [
            SeriesRecord(id="S1", dataset_id="YC", title="t"),
            SeriesRecord(id="S1", dataset_id="YC", title="t"),  # duplicate → raises
        ]
        with pytest.raises(ValueError):
            write_series(rows_bad, tmp_path, "ECB", "YC")
        final = tmp_path / "ECB" / "series" / "YC.parquet"
        assert not final.exists()

    def test_tmp_file_cleaned_up_on_failure(self, tmp_path: Path) -> None:
        rows_bad = [
            SeriesRecord(id="S1", dataset_id="YC", title="t"),
            SeriesRecord(id="S1", dataset_id="YC", title="t"),
        ]
        with pytest.raises(ValueError):
            write_series(rows_bad, tmp_path, "ECB", "YC")
        tmp_dir = tmp_path / "ECB" / TMP_SUBDIR
        if tmp_dir.exists():
            leftovers = list(tmp_dir.iterdir())
            assert leftovers == [], f"tmp files left behind: {leftovers}"

    def test_tmp_dir_is_per_agency(self, tmp_path: Path) -> None:
        """Orphan sweep in T11 walks per-agency .tmp/; confirm location."""
        rows = [SeriesRecord(id="S1", dataset_id="YC", title="t")]
        seen_tmp_paths: list[str] = []
        real_replace = __import__("os").replace

        def spy(src: str, dst: str) -> None:
            seen_tmp_paths.append(str(src))
            real_replace(src, dst)

        with patch("os.replace", side_effect=spy):
            write_series(rows, tmp_path, "ECB", "YC")

        assert len(seen_tmp_paths) == 1
        assert f"/ECB/{TMP_SUBDIR}/" in seen_tmp_paths[0]

    def test_replace_not_direct_write(self, tmp_path: Path) -> None:
        """The canonical path should never exist as an open file handle — only via rename."""
        rows = [SeriesRecord(id="S1", dataset_id="YC", title="t")]
        replace_calls: list[tuple[str, str]] = []
        real_replace = __import__("os").replace

        def spy_replace(src: str, dst: str) -> None:
            replace_calls.append((str(src), str(dst)))
            real_replace(src, dst)

        with patch("os.replace", side_effect=spy_replace):
            write_series(rows, tmp_path, "ECB", "YC")

        assert len(replace_calls) == 1
        src, dst = replace_calls[0]
        assert TMP_SUBDIR in src
        assert dst.endswith("YC.parquet")
