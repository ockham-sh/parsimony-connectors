"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_finnhub


def test_connectors_count() -> None:
    assert len(parsimony_finnhub.CONNECTORS) == 12


def test_minimal_public_surface() -> None:
    # CONNECTORS is the contract; load() is the per-package key-binding idiom.
    assert parsimony_finnhub.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_finnhub, "CONNECTORS")
    assert hasattr(parsimony_finnhub, "load")
