from pathlib import Path

import pytest

from parsimony_sdmx._isolation.layout import (
    agency_dir,
    datasets_parquet,
    structure_json,
    tmp_dir,
)


class TestLayoutPaths:
    def test_agency_dir(self) -> None:
        assert agency_dir(Path("/out"), "ECB") == Path("/out/ECB")

    def test_datasets_parquet(self) -> None:
        assert datasets_parquet(Path("/out"), "ECB") == Path("/out/ECB/datasets.parquet")

    def test_structure_json(self) -> None:
        assert structure_json(Path("/out"), "ECB", "YC") == Path("/out/ECB/structure/YC.json")

    def test_tmp_dir(self) -> None:
        base = Path("/out")
        assert tmp_dir(base, "ECB") == Path("/out/ECB/.tmp")

    def test_rejects_path_traversal(self) -> None:
        with pytest.raises(ValueError):
            structure_json(Path("/out"), "ECB", "../evil")
