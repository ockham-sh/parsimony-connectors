"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_tiingo


def test_connectors_count() -> None:
    assert len(parsimony_tiingo.CONNECTORS) == 13


def test_minimal_public_surface() -> None:
    # CONNECTORS is the contract; load() is the per-package key-binding idiom.
    assert parsimony_tiingo.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_tiingo, "CONNECTORS")
    assert hasattr(parsimony_tiingo, "load")
