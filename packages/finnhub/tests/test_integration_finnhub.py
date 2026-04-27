"""Live integration tests for parsimony-finnhub."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_finnhub import FinnhubSearchParams, finnhub_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_finnhub_search_apple() -> None:
    creds = require_env("FINNHUB_API_KEY")
    bound = finnhub_search.bind(api_key=creds["FINNHUB_API_KEY"])

    result = await bound(FinnhubSearchParams(query="apple"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Finnhub search for 'apple' returned empty DataFrame"
