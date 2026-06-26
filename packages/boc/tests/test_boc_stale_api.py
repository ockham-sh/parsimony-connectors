"""Guard against reintroducing removed BOC catalog load APIs."""

from __future__ import annotations

from pathlib import Path


def test_boc_runtime_has_no_fallback_catalog_params() -> None:
    root = Path(__file__).resolve().parents[1] / "parsimony_boc"
    text = "\n".join(path.read_text() for path in root.rglob("*.py"))
    assert "fallback_bm25" not in text
    assert "fallback_enumerator" not in text
