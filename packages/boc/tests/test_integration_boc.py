"""Live integration tests for parsimony-boc (Bank of Canada)."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_boc import BocFetchParams, boc_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_boc_fetch_usd_cad_fx() -> None:
    # FXUSDCAD — USD/CAD closing rate — is one of BoC's oldest stable series.
    result = await boc_fetch(BocFetchParams(series_name="FXUSDCAD"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "BoC fetch of FXUSDCAD returned empty DataFrame"
