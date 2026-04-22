"""Happy-path tests for the EIA connectors.

Follows ``docs/testing-template.md``. EIA auth is ``?api_key=<key>`` via
``HttpClient(query_params=...)``; error-mapping added in this sweep
(previously ``response.raise_for_status()`` leaked raw ``httpx`` errors).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, RateLimitError, UnauthorizedError

from parsimony_eia import (
    CONNECTORS,
    EiaFetchParams,
    eia_fetch,
)

_KEY = "live-looking-eia-xyz"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_env_vars_maps_api_key() -> None:
    assert CONNECTORS["eia_fetch"].env_map == {"api_key": "EIA_API_KEY"}


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"eia_fetch", "enumerate_eia"}


# ---------------------------------------------------------------------------
# eia_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_eia_fetch_returns_rows() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "description": "Spot Prices",
                    "data": [
                        {"period": "2026-03", "value": 78.5, "duoarea": "NUS", "product": "EPCBRENT"},
                        {"period": "2026-02", "value": 77.0, "duoarea": "NUS", "product": "EPCBRENT"},
                    ],
                }
            },
        )
    )

    bound = eia_fetch.bind(api_key=_KEY)
    result = await bound(EiaFetchParams(route="petroleum/pri/spt"))

    assert result.provenance.source == "eia"
    assert len(result.data) == 2
    assert result.data.iloc[0]["title"] == "Spot Prices"


@respx.mock
@pytest.mark.asyncio
async def test_eia_fetch_maps_401_without_leaking_key() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(401, text="invalid api key")
    )

    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(EiaFetchParams(route="petroleum/pri/spt"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_eia_fetch_maps_429_without_leaking_key() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "30"}, text="too many")
    )

    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(EiaFetchParams(route="petroleum/pri/spt"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_eia_fetch_raises_empty_data_when_no_records() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(200, json={"response": {"data": []}})
    )

    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        await bound(EiaFetchParams(route="petroleum/pri/spt"))


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_fetch_rejects_empty_route() -> None:
    with pytest.raises(ValueError):
        EiaFetchParams(route="   ")
