"""Live integration tests for parsimony-polymarket."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_polymarket import polymarket_events, polymarket_markets

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_polymarket_events_live() -> None:
    result = await polymarket_events(limit=5)
    assert_provenance_shape(result, expected_source="polymarket_events")
    assert not result.data.empty


@pytest.mark.asyncio
async def test_polymarket_markets_live() -> None:
    result = await polymarket_markets(limit=5)
    assert_provenance_shape(result, expected_source="polymarket_markets")
    assert not result.data.empty
