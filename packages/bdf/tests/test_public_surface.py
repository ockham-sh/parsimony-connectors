"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_bdf


def test_connectors_count() -> None:
    assert len(parsimony_bdf.CONNECTORS) == 3


def test_minimal_public_surface() -> None:
    assert parsimony_bdf.__all__ == ["CONNECTORS"]
    assert hasattr(parsimony_bdf, "CONNECTORS")
