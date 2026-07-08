from pathlib import Path

import pytest

from parsimony_sdmx._isolation.layout import (
    agency_dir,
    structure_json,
)


class TestLayoutPaths:
    def test_agency_dir(self) -> None:
        assert agency_dir(Path("/out"), "ECB") == Path("/out/ECB")

    def test_structure_json(self) -> None:
        assert structure_json(Path("/out"), "ECB", "YC") == Path("/out/ECB/structure/YC.json")

    def test_rejects_path_traversal(self) -> None:
        with pytest.raises(ValueError):
            structure_json(Path("/out"), "ECB", "../evil")
