"""Error-mapping contract for parsimony-finnhub.

Finnhub uses a custom mapper (not ``parsimony.transport.map_http_error``)
that differs from the canonical table on the premium-required path:
finnhub returns 403 (not 402) for premium-only endpoints. We drop 402
from the parametrized suite and add an explicit 403 assertion.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony_test_support import CANARY_KEY, ErrorMappingSuite, assert_no_secret_leak

from parsimony_finnhub import FinnhubSearchParams, finnhub_search

_ROUTE = "https://finnhub.io/api/v1/search"


class TestFinnhubSearchErrorMapping(ErrorMappingSuite):
    connector = finnhub_search
    params = FinnhubSearchParams(query="apple")
    route_url = _ROUTE
    provider = "finnhub"
    # Drop 402 from the canonical table — finnhub maps 403 → PaymentRequired.
    status_map = [
        (401, UnauthorizedError),
        (429, RateLimitError),
        (500, ProviderError),
        (503, ProviderError),
    ]


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_403_maps_to_payment_required() -> None:
    """Finnhub's premium-required response is 403 (not 402)."""
    respx.get(_ROUTE).mock(return_value=httpx.Response(403, text="premium only"))

    bound = finnhub_search.bind(api_key=CANARY_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        await bound(FinnhubSearchParams(query="apple"))

    assert_no_secret_leak(exc_info.value)
    assert exc_info.value.provider == "finnhub"
