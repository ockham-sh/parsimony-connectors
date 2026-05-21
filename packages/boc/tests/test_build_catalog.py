"""Tests for BoC catalog build script configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from parsimony.catalog import HybridIndex
from parsimony.ranking import ZScoreFusion

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_catalog.py"


def _build_module():
    spec = importlib.util.spec_from_file_location("boc_build_catalog", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_catalog_uses_current_hybrid_index_api() -> None:
    catalog = _build_module()._catalog()
    assert catalog.name == "boc"
    assert catalog.default_field == "title"
    assert len(catalog._indexes) == 3

    title_idx = catalog.index_for("title")
    assert isinstance(title_idx, HybridIndex)
    fusion = title_idx._fusion
    assert isinstance(fusion, ZScoreFusion)
    assert fusion.weights["title_bm25"] == 0.6
    assert fusion.weights["title_vector"] == 1.0

    description_idx = catalog.index_for("description")
    assert isinstance(description_idx, HybridIndex)
    desc_fusion = description_idx._fusion
    assert isinstance(desc_fusion, ZScoreFusion)
    assert desc_fusion.weights["description_bm25"] == 0.5
