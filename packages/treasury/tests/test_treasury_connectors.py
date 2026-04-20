"""Happy-path tests for the US Treasury Fiscal Data connectors.

Follows ``docs/testing-template.md``: respx-mocked upstream, assertions limited to
the public ``Result`` surface. Treasury has no ``api_key`` dep, so the
401/429 error-mapping tests in the template do not apply here.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_treasury import (
    CONNECTORS,
    TreasuryEnumerateParams,
    TreasuryFetchParams,
    enumerate_treasury,
    treasury_fetch,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"treasury_fetch", "enumerate_treasury"}


def test_treasury_fetch_tags() -> None:
    fetch = next(c for c in CONNECTORS if c.name == "treasury_fetch")
    assert {"macro", "us"} <= set(fetch.tags)


# ---------------------------------------------------------------------------
# treasury_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_treasury_fetch_returns_records() -> None:
    respx.get(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "record_date": "2026-01-02",
                        "tot_pub_debt_out_amt": "34100000000000",
                        "debt_held_public_amt": "27000000000000",
                        "intragov_hold_amt": "7100000000000",
                    },
                ],
                "meta": {
                    "total-count": 1,
                    "labels": {"record_date": "Debt to the Penny"},
                    "dataTypes": {
                        "tot_pub_debt_out_amt": "CURRENCY",
                        "debt_held_public_amt": "CURRENCY",
                        "intragov_hold_amt": "CURRENCY",
                    },
                },
            },
        )
    )

    result = await treasury_fetch(TreasuryFetchParams(endpoint="v2/accounting/od/debt_to_penny"))

    assert result.provenance.source == "treasury"
    assert result.provenance.params == {"endpoint": "v2/accounting/od/debt_to_penny"}
    df = result.data
    assert "record_date" in df.columns
    assert "endpoint" in df.columns
    assert list(df["endpoint"]) == ["v2/accounting/od/debt_to_penny"]


@respx.mock
@pytest.mark.asyncio
async def test_treasury_fetch_raises_empty_data_on_no_records() -> None:
    respx.get(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
    ).mock(return_value=httpx.Response(200, json={"data": [], "meta": {}}))

    with pytest.raises(EmptyDataError):
        await treasury_fetch(TreasuryFetchParams(endpoint="v2/accounting/od/debt_to_penny"))


# ---------------------------------------------------------------------------
# enumerate_treasury
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_treasury_emits_catalog_rows() -> None:
    respx.get("https://api.fiscaldata.treasury.gov/services/dtg/metadata/").mock(
        return_value=httpx.Response(
            200,
            json={
                "datasets": [
                    {
                        "dataset_name": "Debt to the Penny",
                        "publisher": "Bureau of the Fiscal Service",
                        "update_frequency": "Daily",
                        "apis": [
                            {
                                "endpoint_txt": "/services/api/fiscal_service/v2/accounting/od/debt_to_penny",
                                "table_name": "Debt to the Penny",
                                "api_id": "debt_to_penny",
                            },
                        ],
                    },
                ],
            },
        )
    )

    result = await enumerate_treasury(TreasuryEnumerateParams())

    df = result.data
    assert len(df) == 1
    assert df["endpoint"].iloc[0] == "v2/accounting/od/debt_to_penny"
    assert df["title"].iloc[0] == "Debt to the Penny"


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_treasury_fetch_params_bounds_page_size() -> None:
    with pytest.raises(ValueError):
        TreasuryFetchParams(endpoint="x", page_size=0)
    with pytest.raises(ValueError):
        TreasuryFetchParams(endpoint="x", page_size=10001)


def test_treasury_fetch_params_accepts_filter_and_sort() -> None:
    p = TreasuryFetchParams(
        endpoint="v2/accounting/od/debt_to_penny",
        filter="record_date:gte:2026-01-01",
        sort="-record_date",
        page_size=50,
    )
    assert p.filter == "record_date:gte:2026-01-01"
    assert p.sort == "-record_date"
