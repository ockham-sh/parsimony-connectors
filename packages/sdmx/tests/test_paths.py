from pathlib import Path

import pytest

from parsimony_sdmx.io.paths import ensure_within, safe_filename


class TestSafeFilename:
    @pytest.mark.parametrize(
        "name",
        [
            "ECB",
            "YC",
            "ECB-YC",
            "name.with.dots",
            "NAMA_10_GDP",
            "a",
            "A-b_c.1",
            # ESTAT pseudo-dataflows use ``$`` as a derivation separator.
            "LFST_HHEREDCH$DV_1343",
        ],
    )
    def test_accepts_valid_names(self, name: str) -> None:
        assert safe_filename(name) == name

    @pytest.mark.parametrize(
        "name",
        [
            "",
            ".",
            "..",
            "foo/bar",
            "foo\\bar",
            "foo\x00bar",
            "foo bar",
            "foo*bar",
            "foo;bar",
            "foo'bar",
            "foo\"bar",
            "<script>",
            "a" * 201,
        ],
    )
    def test_rejects_unsafe_names(self, name: str) -> None:
        with pytest.raises(ValueError):
            safe_filename(name)


class TestEnsureWithin:
    def test_allows_descendant(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b"
        target.mkdir(parents=True)
        assert ensure_within(tmp_path, target) == target.resolve()

    def test_rejects_parent_traversal(self, tmp_path: Path) -> None:
        outside = tmp_path.parent
        with pytest.raises(ValueError):
            ensure_within(tmp_path, outside)

    def test_rejects_sibling(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            ensure_within(tmp_path / "base", tmp_path / "other")

    def test_rejects_dotdot_escape(self, tmp_path: Path) -> None:
        base = tmp_path / "base"
        base.mkdir()
        escape = base / ".." / ".." / "etc"
        with pytest.raises(ValueError):
            ensure_within(base, escape)
