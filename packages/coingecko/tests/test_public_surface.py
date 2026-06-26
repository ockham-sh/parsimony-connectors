"""Public-surface contract: minimal export surface."""

from __future__ import annotations

import parsimony_coingecko


def test_connectors_count() -> None:
    assert len(parsimony_coingecko.CONNECTORS) == 11


def test_minimal_public_surface() -> None:
    assert parsimony_coingecko.__all__ == ["CONNECTORS", "load"]
    assert hasattr(parsimony_coingecko, "CONNECTORS")
    assert hasattr(parsimony_coingecko, "load")
