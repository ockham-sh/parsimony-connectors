"""Live integration tests for parsimony-rba (Reserve Bank of Australia)."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_rba import RbaFetchParams, rba_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_rba_fetch_cash_rate() -> None:
    # F1 is the RBA Cash Rate Target table — the canonical RBA dataset.
    result = await rba_fetch(RbaFetchParams(table_id="f1-data"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "RBA fetch of f1-data returned empty DataFrame"
