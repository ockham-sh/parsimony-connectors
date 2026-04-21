"""Unified error-mapping contract — parametric across statuses and code paths.

Every FMP connector — simple or screener — routes through the same error
mapper in ``parsimony_fmp._http``. This file pins two invariants:

1. Status → typed exception (401 → UnauthorizedError, 402 → PaymentRequiredError,
   429 → RateLimitError, 5xx → ProviderError).
2. The API key string never appears in any raised exception's ``str()``.

Two code paths are exercised — ``fmp_fetch`` (simple connector, 18 callers)
and the screener's multi-endpoint orchestration. Both go through
``_raise_mapped_error`` so one test each is sufficient to cover the whole
19-connector surface.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import (
    ConnectorError,
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)

from parsimony_fmp import FmpScreenerParams, FmpSearchParams, fmp_screener, fmp_search

_KEY = "live-looking-fmp-key-do-not-leak"

_STATUS_TO_EXCEPTION: list[tuple[int, type[ConnectorError]]] = [
    (401, UnauthorizedError),
    (402, PaymentRequiredError),
    (429, RateLimitError),
    (500, ProviderError),
    (503, ProviderError),
]


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize("status,exc_type", _STATUS_TO_EXCEPTION)
async def test_simple_connector_maps_status_and_does_not_leak_key(
    status: int, exc_type: type[ConnectorError]
) -> None:
    """Exercises ``fmp_fetch`` → ``_raise_mapped_error``."""
    respx.get("https://financialmodelingprep.com/stable/search-name").mock(
        return_value=httpx.Response(status, text="provider said no")
    )

    bound = fmp_search.bind_deps(api_key=_KEY)
    with pytest.raises(exc_type) as exc_info:
        await bound(FmpSearchParams(query="x"))

    assert _KEY not in str(exc_info.value), f"api_key leaked via {exc_type.__name__} on {status}"
    assert exc_info.value.provider == "fmp"


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize("status,exc_type", _STATUS_TO_EXCEPTION)
async def test_screener_initial_fetch_maps_status_and_does_not_leak_key(
    status: int, exc_type: type[ConnectorError]
) -> None:
    """Exercises the screener's first-call path through the unified mapper."""
    respx.get("https://financialmodelingprep.com/stable/company-screener").mock(
        return_value=httpx.Response(status, text="provider said no")
    )

    bound = fmp_screener.bind_deps(api_key=_KEY)
    with pytest.raises(exc_type) as exc_info:
        await bound(FmpScreenerParams(sector="Technology"))

    assert _KEY not in str(exc_info.value), f"api_key leaked via {exc_type.__name__} on {status}"
    assert exc_info.value.provider == "fmp"


@respx.mock
@pytest.mark.asyncio
async def test_rate_limit_error_carries_retry_after() -> None:
    """A 429 response's Retry-After header should populate ``RateLimitError.retry_after``."""
    respx.get("https://financialmodelingprep.com/stable/search-name").mock(
        return_value=httpx.Response(429, text="too fast", headers={"Retry-After": "42"})
    )
    bound = fmp_search.bind_deps(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(FmpSearchParams(query="x"))
    assert exc_info.value.retry_after == 42.0


@respx.mock
@pytest.mark.asyncio
async def test_provider_error_carries_status_code() -> None:
    respx.get("https://financialmodelingprep.com/stable/search-name").mock(
        return_value=httpx.Response(503, text="unavailable")
    )
    bound = fmp_search.bind_deps(api_key=_KEY)
    with pytest.raises(ProviderError) as exc_info:
        await bound(FmpSearchParams(query="x"))
    assert exc_info.value.status_code == 503
