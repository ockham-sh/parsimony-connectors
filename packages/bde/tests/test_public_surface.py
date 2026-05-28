"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_bde


def test_connectors_count() -> None:
    assert len(parsimony_bde.CONNECTORS) == 3


def test_minimal_public_surface() -> None:
    assert parsimony_bde.__all__ == ["CONNECTORS"]
    assert hasattr(parsimony_bde, "CONNECTORS")
    assert not hasattr(parsimony_bde, "bde_fetch")
