from pathlib import Path

import pytest

from parsimony_sdmx._isolation.layout import (
    agency_dir,
    datasets_parquet,
    oom_dir,
    series_parquet,
    tmp_dir,
)


class TestLayout:
    def test_agency_dir(self) -> None:
        assert agency_dir(Path("/out"), "ECB") == Path("/out/ECB")

    def test_datasets_parquet(self) -> None:
        assert datasets_parquet(Path("/out"), "ECB") == Path("/out/ECB/datasets.parquet")

    def test_series_parquet(self) -> None:
        assert series_parquet(Path("/out"), "ECB", "YC") == Path(
            "/out/ECB/series/YC.parquet"
        )

    def test_tmp_and_oom(self) -> None:
        base = Path("/out")
        assert tmp_dir(base, "ECB") == Path("/out/ECB/.tmp")
        assert oom_dir(base, "ECB") == Path("/out/ECB/.oom")

    def test_unsafe_agency_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            agency_dir(Path("/out"), "../evil")

    def test_unsafe_dataset_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            series_parquet(Path("/out"), "ECB", "../evil")
