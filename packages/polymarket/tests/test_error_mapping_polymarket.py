"""Error-mapping contract for parsimony-polymarket.

Polymarket has no ``api_key`` dependency — 401 is not a meaningful
response for public endpoints, but every other status in the canonical
mapping is still routed through ``map_http_error`` by
``parsimony.transport.HttpClient`` and must produce the typed error.

We still assert no secret leaks even though there's no API key, because
the canonical check covers arbitrary sensitive headers / query params
that may be introduced later.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import (
    ConnectorError,
    ProviderError,
    RateLimitError,
)
from parsimony_test_support import CANARY_KEY, STATUS_TO_EXC, assert_no_secret_leak

from parsimony_polymarket import POLYMARKET_GAMMA, PolymarketFetchParams

_GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize("status,exc_type", STATUS_TO_EXC)
async def test_polymarket_gamma_maps_status(
    status: int, exc_type: type[ConnectorError]
) -> None:
    respx.get(_GAMMA_EVENTS_URL).mock(
        return_value=httpx.Response(status, text=f"status={status}")
    )

    with pytest.raises(exc_type) as exc_info:
        await POLYMARKET_GAMMA(PolymarketFetchParams(path="/events"))

    # Nothing sensitive is sent on the wire yet, but pin the structural
    # invariant so future additions of auth / proxy headers trip the test.
    assert_no_secret_leak(exc_info.value, secret=CANARY_KEY)


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_rate_limit_carries_retry_after() -> None:
    respx.get(_GAMMA_EVENTS_URL).mock(
        return_value=httpx.Response(429, text="slow down", headers={"Retry-After": "15"})
    )

    with pytest.raises(RateLimitError) as exc_info:
        await POLYMARKET_GAMMA(PolymarketFetchParams(path="/events"))
    assert exc_info.value.retry_after == 15.0


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_provider_error_carries_status_code() -> None:
    respx.get(_GAMMA_EVENTS_URL).mock(return_value=httpx.Response(503, text="unavailable"))

    with pytest.raises(ProviderError) as exc_info:
        await POLYMARKET_GAMMA(PolymarketFetchParams(path="/events"))
    assert exc_info.value.status_code == 503
