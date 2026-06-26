"""Tests for BoC catalog build script configuration."""

from __future__ import annotations

import sys
from pathlib import Path

from parsimony.catalog import BM25Index, Catalog, Entity, HybridIndex

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT / "tooling") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "tooling"))

from parsimony.catalog.policy import discovery_indexes as macro_discovery_indexes  # noqa: E402


def _sample_entries() -> list[Entity]:
    return [
        Entity(namespace="boc", code="v62318738", title="GDP growth", metadata={"description": "Real GDP"}),
        Entity(namespace="boc", code="v62318739", title="CPI index", metadata={"description": "Consumer prices"}),
    ]


def test_macro_discovery_indexes_for_boc_sample() -> None:
    entries = _sample_entries()
    indexes = macro_discovery_indexes(entries)
    catalog = Catalog("boc", indexes=indexes, default_field="title")
    assert catalog.name == "boc"
    assert catalog.default_field == "title"
    assert set(indexes) == {"code", "title", "description"}
    assert isinstance(indexes["code"], BM25Index)
    assert isinstance(indexes["title"], HybridIndex)
    assert isinstance(indexes["description"], HybridIndex)
