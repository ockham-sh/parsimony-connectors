"""Live integration tests for parsimony-riksbank.

``RIKSBANK_API_KEY`` is not available in the workspace ``.env`` at the
time of writing; the integration test will ``pytest.skip`` until it is
provisioned. The test body remains wired so that adding the key is the
only step required to re-enable coverage.
"""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_riksbank import RiksbankFetchParams, riksbank_fetch

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_riksbank_fetch_sekeur() -> None:
    creds = require_env("RIKSBANK_API_KEY")
    bound = riksbank_fetch.bind(api_key=creds["RIKSBANK_API_KEY"])

    result = await bound(RiksbankFetchParams(series_id="SEKEURPMI"))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Riksbank fetch of SEKEURPMI returned empty DataFrame"
