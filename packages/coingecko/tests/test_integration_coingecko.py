"""Live integration tests for parsimony-coingecko."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_coingecko import CoinGeckoSearchParams, coingecko_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_coingecko_search_btc() -> None:
    creds = require_env("COINGECKO_API_KEY")
    bound = coingecko_search.bind(api_key=creds["COINGECKO_API_KEY"])

    result = await bound(CoinGeckoSearchParams(query="bitcoin"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "CoinGecko search for 'bitcoin' returned empty DataFrame"
