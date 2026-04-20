"""Happy-path tests for the Banque de France connectors.

BdF requires an api_key (X-IBM-Client-Id header). The connector calls
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
    ENV_VARS,
    BdfFetchParams,
    bdf_fetch,
)

_KEY = "live-looking-bdf-client-id"

_BDF_CSV = (
    "SERIES_KEY;TIME_PERIOD;OBS_VALUE;SERIES_TITLE\n"
    "EXR.M.USD.EUR.SP00.E;2026-01;1.0832;USD/EUR exchange rate\n"
    "EXR.M.USD.EUR.SP00.E;2026-02;1.0874;USD/EUR exchange rate\n"
)


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "BANQUEDEFRANCE_KEY"}


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bdf_fetch", "enumerate_bdf"}


@respx.mock
@pytest.mark.asyncio
async def test_bdf_fetch_parses_csv_response() -> None:
    respx.get(
        "https://api.webstat.banque-france.fr/webstat-en/v1/data/EXR.M.USD.EUR.SP00.E"
    ).mock(return_value=httpx.Response(200, text=_BDF_CSV))

    bound = bdf_fetch.bind_deps(api_key=_KEY)
    result = await bound(BdfFetchParams(key="EXR.M.USD.EUR.SP00.E"))

    assert result.provenance.source == "bdf"
    # api_key never propagates into provenance (it's passed as a request header)
    assert _KEY not in str(result.provenance.model_dump())
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "USD/EUR exchange rate"


@respx.mock
@pytest.mark.asyncio
async def test_bdf_fetch_raises_empty_data_on_header_only_csv() -> None:
    respx.get(
        "https://api.webstat.banque-france.fr/webstat-en/v1/data/XX"
    ).mock(
        return_value=httpx.Response(
            200, text="SERIES_KEY;TIME_PERIOD;OBS_VALUE;SERIES_TITLE\n"
        )
    )

    bound = bdf_fetch.bind_deps(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        await bound(BdfFetchParams(key="XX"))


def test_fetch_rejects_empty_key() -> None:
    with pytest.raises(ValueError):
        BdfFetchParams(key="   ")
