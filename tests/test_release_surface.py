"""Tests for catalog release-surface audit helpers."""

from __future__ import annotations

import json
from pathlib import Path

from catalog_validate.release_surface import (
    audit_local_root,
    classify_bundle_name,
    is_publishable_local_bundle,
    unreferenced_files_in_bundle,
)


def test_classify_sdmx_structure_is_excess() -> None:
    assert classify_bundle_name("sdmx_structure_ecb_yc") == "excess"
    assert classify_bundle_name("sdmx_datasets_ecb") == "sdmx_datasets"
    assert classify_bundle_name("sdmx_series_ecb_yc") == "sdmx_series"


def test_is_publishable_rejects_structure_markers(tmp_path: Path) -> None:
    marker = tmp_path / "sdmx_structure_ecb_yc"
    marker.mkdir()
    (marker / "meta.json").write_text("{}", encoding="utf-8")
    assert not is_publishable_local_bundle(marker)


def test_unreferenced_files_detects_stale_index_dir(tmp_path: Path) -> None:
    bundle = tmp_path / "treasury"
    bundle.mkdir()
    (bundle / "meta.json").write_text(
        json.dumps(
            {
                "index_fields": {"code": "bm25", "title": "bm25"},
                "backend": {"rows_filename": "entries.parquet"},
            }
        ),
        encoding="utf-8",
    )
    (bundle / "entries.parquet").write_bytes(b"")
    (bundle / "indexes" / "code").mkdir(parents=True)
    (bundle / "indexes" / "code" / "meta.json").write_text("{}", encoding="utf-8")
    (bundle / "indexes" / "title_bm25").mkdir()
    (bundle / "indexes" / "title_bm25" / "meta.json").write_text("{}", encoding="utf-8")
    extras = unreferenced_files_in_bundle(bundle)
    assert any("title_bm25" in x for x in extras)


def test_audit_local_root_flags_sdmx_root_excess(tmp_path: Path) -> None:
    sdmx = tmp_path / "sdmx"
    sdmx.mkdir()
    (sdmx / "meta.json").write_text("{}", encoding="utf-8")
    for agency in ("ecb", "estat", "imf_data", "wb_wdi"):
        ds = sdmx / f"sdmx_datasets_{agency}"
        ds.mkdir()
        (ds / "meta.json").write_text("{}", encoding="utf-8")
    # flat stubs
    for provider in ("treasury", "riksbank", "rba", "bde", "boc", "destatis", "snb", "bdp", "bdf", "eia"):
        p = tmp_path / provider
        p.mkdir()
        (p / "meta.json").write_text("{}", encoding="utf-8")
    boj = tmp_path / "boj" / "boj_databases"
    boj.mkdir(parents=True)
    (boj / "meta.json").write_text("{}", encoding="utf-8")

    report = audit_local_root(tmp_path, require_bdf=True)
    assert "sdmx/meta.json" in report.excess
    assert report.ok
