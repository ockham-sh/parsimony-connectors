"""Live integration tests for parsimony-eodhd."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_eodhd import EodhdSearchParams, eodhd_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_eodhd_search_apple() -> None:
    creds = require_env("EODHD_API_KEY")
    bound = eodhd_search.bind(api_key=creds["EODHD_API_KEY"])

    result = await bound(EodhdSearchParams(query="apple"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "EODHD search for 'apple' returned empty DataFrame"
