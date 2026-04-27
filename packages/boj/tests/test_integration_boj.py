"""Live integration tests for parsimony-boj (Bank of Japan)."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_boj import BojFetchParams, boj_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_boj_fetch_fx_rate() -> None:
    # FM08 / FXERD01 — BoJ foreign-exchange rates — stable public series.
    result = await boj_fetch(BojFetchParams(db="FM08", code="FXERD01"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "BoJ fetch of FM08/FXERD01 returned empty DataFrame"
