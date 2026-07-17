"""Release-root hygiene: only series/datasets catalogs ship."""

from __future__ import annotations

from pathlib import Path

from parsimony_sdmx.catalog_manifest import BuildRoot
from parsimony_sdmx.release_validate import validate_release_root


def test_structures_live_under_staging(tmp_path: Path) -> None:
    layout = BuildRoot.create(tmp_path)
    assert layout.structures == tmp_path / "staging" / "structures"
    assert layout.structures.is_dir()


def test_release_root_rejects_build_artifacts(tmp_path: Path) -> None:
    layout = BuildRoot.create(tmp_path)
    (layout.catalogs / "sdmx_structure_estat_foo").mkdir()
    (layout.catalogs / "sdmx_codelist_ecb_bar").mkdir()

    report = validate_release_root(layout, require_all_agencies=False, require_series_agencies=False)

    assert not report.ok
    unexpected = [e for e in report.errors if "unexpected directory" in e]
    assert len(unexpected) == 2
