"""Live integration tests for parsimony-eia."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_eia import EiaFetchParams, eia_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_eia_fetch_petroleum_spot_prices() -> None:
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    # petroleum/pri/spt — spot prices — is a stable EIA v2 route.
    result = await bound(EiaFetchParams(route="petroleum/pri/spt"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "EIA fetch of petroleum/pri/spt returned empty DataFrame"
