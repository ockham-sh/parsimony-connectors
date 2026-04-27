"""Live integration tests for parsimony-sec-edgar.

SEC EDGAR is public but requires a ``User-Agent`` header identifying
the client (SEC's fair-use policy). The ``edgartools`` library handles
that for us. No API key required.
"""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_sec_edgar import SecEdgarFindCompanyParams, sec_edgar_find_company

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_sec_edgar_find_apple() -> None:
    # "AAPL" is the canonical test case — Apple is CIK 0000320193.
    result = await sec_edgar_find_company(SecEdgarFindCompanyParams(identifier="AAPL"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "sec_edgar_find_company('AAPL') returned empty DataFrame"
