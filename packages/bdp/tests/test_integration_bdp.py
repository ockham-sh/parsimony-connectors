"""Live integration tests for parsimony-bdp (Banco de Portugal)."""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_bdp import BdpEnumerateParams, enumerate_bdp

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_bdp_enumerate_lists_datasets() -> None:
    # Enumerator hits the domain listing endpoint — stable, no params needed.
    result = await enumerate_bdp(BdpEnumerateParams())

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Banco de Portugal enumeration returned empty DataFrame"
