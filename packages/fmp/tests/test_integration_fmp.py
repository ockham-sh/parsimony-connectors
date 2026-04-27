"""Live integration tests for parsimony-fmp.

Hits the real ``https://financialmodelingprep.com/stable`` endpoint.
Skipped by default; run with::

    uv run pytest packages/fmp -m integration

Requires ``FMP_API_KEY`` in the environment.
"""

from __future__ import annotations

import pytest
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_fmp import FmpSearchParams, fmp_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_fmp_search_apple_returns_ticker() -> None:
    creds = require_env("FMP_API_KEY")
    bound = fmp_search.bind(api_key=creds["FMP_API_KEY"])

    result = await bound(FmpSearchParams(query="Apple"))

    # FMP's Provenance.source is the connector name ("fmp_search"), not the
    # provider. Just assert well-formed provenance — don't pin the source.
    assert_provenance_shape(result, required_param_keys=["query"])
    df = result.data
    assert not df.empty, "FMP search for 'Apple' returned empty DataFrame"
    # AAPL is the canonical US ticker; if it's not in the search results,
    # the connector is misconfigured.
    symbols = set(df.get("symbol", []))
    assert "AAPL" in symbols, f"AAPL missing from FMP search: {list(symbols)[:10]}"

    assert_no_secret_leak(result, secret=creds["FMP_API_KEY"])
