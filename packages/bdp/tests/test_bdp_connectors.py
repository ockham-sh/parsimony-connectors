"""Happy-path tests for the Banco de Portugal connectors.

BdP BPstat is public (no api_key); template 401/429 contract does not apply.
The happy-path mock exercises the JSON-stat response shape.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_bdp import CONNECTORS, BdpFetchParams, bdp_fetch


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bdp_fetch", "enumerate_bdp"}


@respx.mock
@pytest.mark.asyncio
async def test_bdp_fetch_parses_json_stat_response() -> None:
    respx.get("https://bpstat.bportugal.pt/data/v1/domains/11/datasets/ABC/").mock(
        return_value=httpx.Response(
            200,
            json={
                "role": {"time": ["Time"]},
                "dimension": {
                    "Time": {
                        "category": {
                            "index": {"2026-01-01": 0, "2026-02-01": 1},
                        }
                    }
                },
                "value": [100.0, 101.5],
                "extension": {
                    "series": [{"id": "s1", "label": "Consumer Prices"}],
                },
            },
        )
    )

    result = await bdp_fetch(BdpFetchParams(domain_id=11, dataset_id="ABC"))

    assert result.provenance.source == "bdp"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "Consumer Prices"


@respx.mock
@pytest.mark.asyncio
async def test_bdp_fetch_raises_empty_data_on_no_observations() -> None:
    respx.get("https://bpstat.bportugal.pt/data/v1/domains/11/datasets/ABC/").mock(
        return_value=httpx.Response(200, json={"role": {}, "dimension": {}, "value": []})
    )

    with pytest.raises(EmptyDataError):
        await bdp_fetch(BdpFetchParams(domain_id=11, dataset_id="ABC"))


def test_fetch_rejects_empty_dataset_id() -> None:
    with pytest.raises(ValueError):
        BdpFetchParams(domain_id=11, dataset_id="   ")
