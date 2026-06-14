"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_eia


def test_connectors_count() -> None:
    assert len(parsimony_eia.CONNECTORS) == 5


def test_minimal_public_surface() -> None:
    assert parsimony_eia.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_eia, "CONNECTORS")
    assert hasattr(parsimony_eia, "load")
    # internal connector symbols are not re-exported at the package root
    assert not hasattr(parsimony_eia, "eia_fetch")
