"""Live integration tests for parsimony-snb (Swiss National Bank)."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_snb import SnbFetchParams, snb_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_snb_fetch_policy_rate() -> None:
    # rendoblim is a stable SNB cube for monetary policy indicators.
    result = await snb_fetch(SnbFetchParams(cube_id="rendoblim"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "SNB fetch of rendoblim returned empty DataFrame"
