from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.io.parquet import (
    DATASETS_SCHEMA,
    TMP_SUBDIR,
    write_datasets,
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


class TestAtomicWrite:
    def test_no_canonical_file_when_writer_fails(self, tmp_path: Path) -> None:
        rows_bad = [
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="t1"),
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="t2"),
        ]
        with pytest.raises(ValueError):
            write_datasets(rows_bad, tmp_path, "ECB")
        final = tmp_path / "ECB" / "datasets.parquet"
        assert not final.exists()

    def test_tmp_file_cleaned_up_on_failure(self, tmp_path: Path) -> None:
        rows_bad = [
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="t1"),
            DatasetRecord(dataset_id="YC", agency_id="ECB", title="t2"),
        ]
        with pytest.raises(ValueError):
            write_datasets(rows_bad, tmp_path, "ECB")
        tmp_dir = tmp_path / "ECB" / TMP_SUBDIR
        if tmp_dir.exists():
            leftovers = list(tmp_dir.iterdir())
            assert leftovers == [], f"tmp files left behind: {leftovers}"

    def test_tmp_dir_is_per_agency(self, tmp_path: Path) -> None:
        rows = [DatasetRecord(dataset_id="YC", agency_id="ECB", title="t")]
        seen_tmp_paths: list[str] = []
        real_replace = __import__("os").replace

        def spy(src: str, dst: str) -> None:
            seen_tmp_paths.append(str(src))
            real_replace(src, dst)

        with patch("os.replace", side_effect=spy):
            write_datasets(rows, tmp_path, "ECB")

        assert len(seen_tmp_paths) == 1
        assert f"/ECB/{TMP_SUBDIR}/" in seen_tmp_paths[0]

    def test_replace_not_direct_write(self, tmp_path: Path) -> None:
        rows = [DatasetRecord(dataset_id="YC", agency_id="ECB", title="t")]
        replace_calls: list[tuple[str, str]] = []
        real_replace = __import__("os").replace

        def spy_replace(src: str, dst: str) -> None:
            replace_calls.append((str(src), str(dst)))
            real_replace(src, dst)

        with patch("os.replace", side_effect=spy_replace):
            write_datasets(rows, tmp_path, "ECB")

        assert len(replace_calls) == 1
        src, dst = replace_calls[0]
        assert TMP_SUBDIR in src
        assert dst.endswith("datasets.parquet")
