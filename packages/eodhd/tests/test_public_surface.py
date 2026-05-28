"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_eodhd


def test_connectors_count() -> None:
    assert len(parsimony_eodhd.CONNECTORS) == 17


def test_minimal_public_surface() -> None:
    assert parsimony_eodhd.__all__ == ["CONNECTORS"]
    assert hasattr(parsimony_eodhd, "CONNECTORS")
