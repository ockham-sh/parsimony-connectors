"""Live integration tests for parsimony-tiingo.

``TIINGO_API_KEY`` is not available in the workspace ``.env`` at the time
of writing; the test will ``pytest.skip`` until it is provisioned.
"""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_tiingo import TiingoSearchParams, tiingo_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_tiingo_search_apple() -> None:
    creds = require_env("TIINGO_API_KEY")
    bound = tiingo_search.bind(api_key=creds["TIINGO_API_KEY"])

    result = await bound(TiingoSearchParams(query="apple"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Tiingo search for 'apple' returned empty DataFrame"
