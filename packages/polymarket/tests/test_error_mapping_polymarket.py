"""Error-mapping contract for parsimony-polymarket."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import ConnectorError, ProviderError, RateLimitError
from parsimony_test_support import CANARY_KEY, STATUS_TO_EXC, assert_no_secret_leak

from parsimony_polymarket import polymarket_events

_GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"


@respx.mock
@pytest.mark.parametrize("status,exc_type", STATUS_TO_EXC)
def test_polymarket_events_maps_status(status: int, exc_type: type[ConnectorError]) -> None:
    respx.get(_GAMMA_EVENTS_URL).mock(return_value=httpx.Response(status, text=f"status={status}"))
    with pytest.raises(exc_type) as exc_info:
        polymarket_events(limit=5)
    assert_no_secret_leak(exc_info.value, secret=CANARY_KEY)


@respx.mock
def test_polymarket_rate_limit_carries_retry_after() -> None:
    respx.get(_GAMMA_EVENTS_URL).mock(return_value=httpx.Response(429, text="slow down", headers={"Retry-After": "15"}))
    with pytest.raises(RateLimitError) as exc_info:
        polymarket_events(limit=5)
    assert exc_info.value.retry_after == 15.0


@respx.mock
def test_polymarket_provider_error_carries_status_code() -> None:
    respx.get(_GAMMA_EVENTS_URL).mock(return_value=httpx.Response(503, text="unavailable"))
    with pytest.raises(ProviderError) as exc_info:
        polymarket_events(limit=5)
    assert exc_info.value.status_code == 503
