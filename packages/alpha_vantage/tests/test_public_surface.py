"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_alpha_vantage


def test_connectors_count() -> None:
    assert len(parsimony_alpha_vantage.CONNECTORS) == 29


def test_minimal_public_surface() -> None:
    assert parsimony_alpha_vantage.__all__ == ["CONNECTORS"]
    assert hasattr(parsimony_alpha_vantage, "CONNECTORS")
    assert not hasattr(parsimony_alpha_vantage, "alpha_vantage_search")
