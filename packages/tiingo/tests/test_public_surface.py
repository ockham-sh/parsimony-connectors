"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_tiingo


def test_connectors_count() -> None:
    assert len(parsimony_tiingo.CONNECTORS) == 13


def test_minimal_public_surface() -> None:
    assert parsimony_tiingo.__all__ == ["CONNECTORS"]
    assert hasattr(parsimony_tiingo, "CONNECTORS")
