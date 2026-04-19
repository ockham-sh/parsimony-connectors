from pathlib import Path

from parsimony_sdmx.cli.layout import oom_dir, tmp_dir
from parsimony_sdmx.cli.orphan_sweep import sweep_orphans


class TestSweepOrphans:
    def test_removes_tmp_files(self, tmp_path: Path) -> None:
        td = tmp_dir(tmp_path, "ECB")
        td.mkdir(parents=True)
        (td / "a.parquet").write_text("x")
        (td / "b.parquet").write_text("y")

        assert sweep_orphans(tmp_path, "ECB") == 2
        assert list(td.iterdir()) == []

    def test_removes_oom_files(self, tmp_path: Path) -> None:
        od = oom_dir(tmp_path, "ECB")
        od.mkdir(parents=True)
        (od / "123.json").write_text("{}")
        (od / "worker.456.json").write_text("{}")

        assert sweep_orphans(tmp_path, "ECB") == 2
        assert list(od.iterdir()) == []

    def test_combined_sweep(self, tmp_path: Path) -> None:
        td = tmp_dir(tmp_path, "ECB")
        od = oom_dir(tmp_path, "ECB")
        td.mkdir(parents=True)
        od.mkdir(parents=True)
        (td / "t.parquet").write_text("x")
        (od / "1.json").write_text("{}")

        assert sweep_orphans(tmp_path, "ECB") == 2

    def test_no_dirs_yet_is_noop(self, tmp_path: Path) -> None:
        assert sweep_orphans(tmp_path, "ECB") == 0

    def test_does_not_recurse_into_subdirs(self, tmp_path: Path) -> None:
        td = tmp_dir(tmp_path, "ECB")
        nested = td / "nested"
        nested.mkdir(parents=True)
        (nested / "deep.parquet").write_text("x")

        # sweep only removes files at the top level of .tmp/
        assert sweep_orphans(tmp_path, "ECB") == 0
        assert (nested / "deep.parquet").exists()
