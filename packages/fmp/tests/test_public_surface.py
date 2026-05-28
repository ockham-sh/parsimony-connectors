"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_fmp


def test_connectors_count() -> None:
    assert len(parsimony_fmp.CONNECTORS) == 19


def test_minimal_public_surface() -> None:
    assert parsimony_fmp.__all__ == ["CONNECTORS"]
    assert hasattr(parsimony_fmp, "CONNECTORS")
    assert not hasattr(parsimony_fmp, "FmpSearchParams")
