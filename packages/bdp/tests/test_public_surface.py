"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_bdp


def test_connectors_count() -> None:
    assert len(parsimony_bdp.CONNECTORS) == 3


def test_minimal_public_surface() -> None:
    assert parsimony_bdp.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_bdp, "CONNECTORS")
    assert hasattr(parsimony_bdp, "load")
