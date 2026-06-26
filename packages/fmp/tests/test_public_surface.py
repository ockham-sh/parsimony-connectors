"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_fmp


def test_connectors_count() -> None:
    assert len(parsimony_fmp.CONNECTORS) == 19


def test_minimal_public_surface() -> None:
    # CONNECTORS is the contract; load() is the per-package key-binding idiom.
    assert parsimony_fmp.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_fmp, "CONNECTORS")
    assert hasattr(parsimony_fmp, "load")
    assert not hasattr(parsimony_fmp, "FmpSearchParams")
