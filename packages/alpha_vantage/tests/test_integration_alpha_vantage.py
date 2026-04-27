"""Live integration tests for parsimony-alpha-vantage."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_alpha_vantage import AlphaVantageSearchParams, alpha_vantage_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_alpha_vantage_search_apple() -> None:
    creds = require_env("ALPHA_VANTAGE_API_KEY")
    bound = alpha_vantage_search.bind(api_key=creds["ALPHA_VANTAGE_API_KEY"])

    result = await bound(AlphaVantageSearchParams(keywords="apple"))

    # Source is the connector name ("alpha_vantage_search"), not the provider.
    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Alpha Vantage search for 'apple' returned empty DataFrame"
