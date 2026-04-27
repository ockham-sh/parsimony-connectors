"""Live integration tests for parsimony-bls."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_bls import BlsFetchParams, bls_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_bls_fetch_unemployment() -> None:
    creds = require_env("BLS_API_KEY")
    bound = bls_fetch.bind(api_key=creds["BLS_API_KEY"])

    # LNS14000000 is the canonical US unemployment rate series from BLS.
    result = await bound(
        BlsFetchParams(series_id="LNS14000000", start_year="2025", end_year="2026")
    )

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "BLS fetch of LNS14000000 returned empty DataFrame"
