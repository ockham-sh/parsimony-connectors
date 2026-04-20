"""Happy-path tests for the SNB connectors.

SNB is public-data (no api_key); template 401/429 contract does not apply.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_snb import CONNECTORS, SnbFetchParams, snb_fetch

_SNB_CSV = (
    "\ufeffdate;value\n"
    "2026-01;108.4\n"
    "2026-02;108.7\n"
)


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"snb_fetch", "enumerate_snb"}


@respx.mock
@pytest.mark.asyncio
async def test_snb_fetch_parses_csv() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text=_SNB_CSV)
    )
    # Dimensions endpoint — returned but not required for the test
    respx.get("https://data.snb.ch/api/cube/rendoblim/dimensions/en").mock(
        return_value=httpx.Response(200, json={"name": "Bond yields"})
    )

    result = await snb_fetch(SnbFetchParams(cube_id="rendoblim"))

    assert result.provenance.source == "snb"
    df = result.data
    assert "cube_id" in df.columns
    assert df.iloc[0]["cube_id"] == "rendoblim"


@respx.mock
@pytest.mark.asyncio
async def test_snb_fetch_raises_empty_data_on_empty_csv() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text="")
    )

    with pytest.raises(EmptyDataError):
        await snb_fetch(SnbFetchParams(cube_id="rendoblim"))


def test_fetch_rejects_empty_cube_id() -> None:
    with pytest.raises(ValueError):
        SnbFetchParams(cube_id="   ")
