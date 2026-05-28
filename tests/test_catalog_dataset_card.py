"""Tests for HF dataset card generation."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from catalog_dataset_card import (
    bundle_catalog_config,
    configs_from_repo_tree,
    flat_catalog_config,
    render_dataset_card,
    strip_frontmatter,
)


def test_flat_frontmatter() -> None:
    card = render_dataset_card(
        repo_id="parsimony-dev/riksbank",
        configs=[flat_catalog_config()],
    )
    assert "config_name: default" in card
    assert "split: train" in card
    assert "path: entries.parquet" in card
    assert "viewer: true" in card


def test_configs_from_multi_bundle_tree(tmp_path: Path) -> None:
    for name in ("boj_databases", "boj_series_fm08"):
        bundle = tmp_path / name
        bundle.mkdir()
        (bundle / "entries.parquet").write_bytes(b"")
        (bundle / "meta.json").write_text("{}", encoding="utf-8")

    configs = configs_from_repo_tree(tmp_path)
    assert [c.config_name for c in configs] == ["boj_databases", "boj_series_fm08"]
    assert configs[0].entry_path == "boj_databases/entries.parquet"
    assert bundle_catalog_config("x").entry_path == "x/entries.parquet"


def test_repo_id_from_hf_url() -> None:
    from publish_catalog_dataset_card import _repo_id_from_hf_url

    assert _repo_id_from_hf_url("hf://parsimony-dev/bde") == "parsimony-dev/bde"
    assert _repo_id_from_hf_url("hf://parsimony-dev/sdmx/") == "parsimony-dev/sdmx"
