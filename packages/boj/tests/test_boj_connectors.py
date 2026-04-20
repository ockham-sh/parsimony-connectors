"""Happy-path tests for the Bank of Japan connectors.

BoJ is public (no api_key); template 401/429 contract does not apply.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_boj import CONNECTORS, BojFetchParams, boj_fetch


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"boj_fetch", "enumerate_boj"}


@respx.mock
@pytest.mark.asyncio
async def test_boj_fetch_returns_observations() -> None:
    respx.get("https://www.stat-search.boj.or.jp/api/v1/getDataCode").mock(
        return_value=httpx.Response(
            200,
            json={
                "RESULTSET": [
                    {
                        "SERIES_CODE": "FXERD01",
                        "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                        "FREQUENCY": "DM",
                        "VALUES": {
                            "SURVEY_DATES": ["20260417", "20260418"],
                            "VALUES": ["152.33", "152.50"],
                        },
                    }
                ]
            },
        )
    )

    result = await boj_fetch(BojFetchParams(db="FM08", code="FXERD01"))

    assert result.provenance.source == "boj"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "JPY/USD Spot Rate"


@respx.mock
@pytest.mark.asyncio
async def test_boj_fetch_raises_empty_data_on_empty_resultset() -> None:
    respx.get("https://www.stat-search.boj.or.jp/api/v1/getDataCode").mock(
        return_value=httpx.Response(200, json={"RESULTSET": []})
    )

    with pytest.raises(EmptyDataError):
        await boj_fetch(BojFetchParams(db="FM08", code="XX"))
