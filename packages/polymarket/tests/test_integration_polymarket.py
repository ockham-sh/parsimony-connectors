"""Live integration tests for parsimony-polymarket.

Polymarket's Gamma + CLOB APIs are public (no API key). These tests run
against the real endpoints when the ``integration`` marker is selected.
"""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_polymarket import POLYMARKET_GAMMA, PolymarketFetchParams

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_polymarket_gamma_events_returns_active_markets() -> None:
    # ``limit`` is an extra field accepted via ``ConfigDict(extra="allow")``;
    # mypy doesn't see it on the BaseModel signature.
    result = await POLYMARKET_GAMMA(PolymarketFetchParams(path="/events", limit=5))  # type: ignore[call-arg]

    assert_provenance_shape(
        result,
        expected_source="polymarket_gamma",
        required_param_keys=["path"],
    )
    df = result.data
    assert not df.empty, "Polymarket gamma /events returned empty DataFrame"
    # Every event has a slug.
    assert "slug" in df.columns, f"Missing 'slug' column: {df.columns.tolist()}"


@pytest.mark.asyncio
async def test_polymarket_gamma_markets_returns_data() -> None:
    result = await POLYMARKET_GAMMA(PolymarketFetchParams(path="/markets", limit=5))  # type: ignore[call-arg]

    assert_provenance_shape(result, expected_source="polymarket_gamma")
    df = result.data
    assert not df.empty, "Polymarket gamma /markets returned empty DataFrame"
