"""Happy-path tests for the Banque de France connectors.

BdF requires an api_key (Authorization: Apikey <KEY>). The connector calls
``response.raise_for_status()`` without explicit 401/429 mapping; the tests
below exercise the happy path + verify the api_key isn't echoed into the
Provenance or parsed response.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_bdf import (
    CONNECTORS,
    BdfFetchParams,
    bdf_fetch,
)

_KEY = "live-looking-bdf-client-id"

_OBSERVATIONS_URL = (
    "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets"
    "/observations/exports/json"
)

_BDF_OBS_JSON = [
    {
        "series_key": "EXR.M.USD.EUR.SP00.E",
        "title_en": "USD/EUR exchange rate",
        "title_fr": "Taux de change USD/EUR",
        "time_period": "2026-01",
        "time_period_start": "2026-01-01",
        "time_period_end": "2026-01-31",
        "obs_value": 1.0832,
        "obs_status": "A",
    },
    {
        "series_key": "EXR.M.USD.EUR.SP00.E",
        "title_en": "USD/EUR exchange rate",
        "title_fr": "Taux de change USD/EUR",
        "time_period": "2026-02",
        "time_period_start": "2026-02-01",
        "time_period_end": "2026-02-28",
        "obs_value": 1.0874,
        "obs_status": "A",
    },
]


def test_env_vars_maps_api_key() -> None:
    assert CONNECTORS["bdf_fetch"].env_map == {"api_key": "BANQUEDEFRANCE_KEY"}


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bdf_fetch", "enumerate_bdf", "bdf_search"}


@respx.mock
@pytest.mark.asyncio
async def test_bdf_fetch_parses_json_response() -> None:
    respx.get(_OBSERVATIONS_URL).mock(
        return_value=httpx.Response(200, json=_BDF_OBS_JSON)
    )

    bound = bdf_fetch.bind(api_key=_KEY)
    result = await bound(BdfFetchParams(key="EXR.M.USD.EUR.SP00.E"))

    assert result.provenance.source == "bdf"
    # api_key never propagates into provenance (it's passed as a request header)
    assert _KEY not in str(result.provenance.model_dump())
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "USD/EUR exchange rate"


@respx.mock
@pytest.mark.asyncio
async def test_bdf_fetch_raises_empty_data_on_no_observations() -> None:
    respx.get(_OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=[]))

    bound = bdf_fetch.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        await bound(BdfFetchParams(key="XX"))


def test_fetch_rejects_empty_key() -> None:
    with pytest.raises(ValueError):
        BdfFetchParams(key="   ")
