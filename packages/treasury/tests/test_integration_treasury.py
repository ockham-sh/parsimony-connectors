"""Live integration tests for parsimony-treasury."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_treasury import TreasuryFetchParams, treasury_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_treasury_fetch_debt_to_penny() -> None:
    # v2/accounting/od/debt_to_penny is a stable, high-traffic Treasury dataset.
    result = await treasury_fetch(
        TreasuryFetchParams(endpoint="v2/accounting/od/debt_to_penny")
    )

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Treasury fetch of debt_to_penny returned empty DataFrame"
