"""Live integration tests for parsimony-bde (Banco de España)."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_bde import BdeFetchParams, bde_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_bde_fetch_known_series() -> None:
    # D_1NBAF472 is a stable BdE series key (policy rate / reference).
    result = await bde_fetch(BdeFetchParams(key="D_1NBAF472"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "BdE fetch returned empty DataFrame"
