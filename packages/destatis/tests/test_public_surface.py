"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_destatis


def test_connectors_count() -> None:
    assert len(parsimony_destatis.CONNECTORS) == 3


def test_minimal_public_surface() -> None:
    assert parsimony_destatis.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_destatis, "CONNECTORS")
    assert hasattr(parsimony_destatis, "load")
    assert not hasattr(parsimony_destatis, "destatis_fetch")
