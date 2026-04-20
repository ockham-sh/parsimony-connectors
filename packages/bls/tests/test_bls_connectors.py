"""Happy-path tests for the BLS connectors.

BLS uses POST with JSON body and returns its own error codes in the response
body rather than HTTP 4xx. The api_key is optional; 401/429 error-mapping
tests from ``docs/testing-template.md`` §4 do not apply (key-bearing is
defined as a key-required dep, and BLS defaults ``api_key=""``).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, ProviderError

from parsimony_bls import (
    CONNECTORS,
    ENV_VARS,
    BlsFetchParams,
    bls_fetch,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "BLS_API_KEY"}


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bls_fetch", "enumerate_bls"}


# ---------------------------------------------------------------------------
# bls_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_bls_fetch_returns_series_observations() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {
                            "seriesID": "LNS14000000",
                            "catalog": {"series_title": "Unemployment Rate"},
                            "data": [
                                {"year": "2026", "period": "M03", "value": "4.1"},
                                {"year": "2026", "period": "M02", "value": "4.0"},
                            ],
                        }
                    ]
                },
            },
        )
    )

    result = await bls_fetch(
        BlsFetchParams(series_id="LNS14000000", start_year="2026", end_year="2026")
    )

    assert result.provenance.source == "bls"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "Unemployment Rate"


@respx.mock
@pytest.mark.asyncio
async def test_bls_fetch_raises_provider_error_on_bls_status_failure() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={"status": "REQUEST_NOT_PROCESSED", "message": ["Invalid series ID"]},
        )
    )

    with pytest.raises(ProviderError):
        await bls_fetch(BlsFetchParams(series_id="BAD", start_year="2026", end_year="2026"))


@respx.mock
@pytest.mark.asyncio
async def test_bls_fetch_raises_empty_data_when_no_series_returned() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={"status": "REQUEST_SUCCEEDED", "Results": {"series": []}},
        )
    )

    with pytest.raises(EmptyDataError):
        await bls_fetch(BlsFetchParams(series_id="XYZ", start_year="2026", end_year="2026"))


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_fetch_rejects_non_four_digit_year() -> None:
    with pytest.raises(ValueError):
        BlsFetchParams(series_id="X", start_year="26", end_year="2026")


def test_fetch_rejects_empty_series_id() -> None:
    with pytest.raises(ValueError):
        BlsFetchParams(series_id="   ", start_year="2026", end_year="2026")
