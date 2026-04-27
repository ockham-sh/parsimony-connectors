"""Reusable pytest suite classes — subclass per connector.

These mixins encode the two most-repeated test patterns in the monorepo:

* :class:`ErrorMappingSuite` — canonical HTTP-status → ConnectorError
  mapping plus the Retry-After header contract plus secret-leak defense.
* :class:`IntegrationSuite` — live-API smoke test wired to
  ``require_env``, skipped when credentials are missing.

Usage (connectors with an API key)::

    from parsimony_fred import fred_search, FredSearchParams
    from parsimony_test_support import CANARY_KEY
    from parsimony_test_support.suites import ErrorMappingSuite

    class TestFredErrorMapping(ErrorMappingSuite):
        connector = fred_search
        params = FredSearchParams(search_text="x")
        route_url = "https://api.stlouisfed.org/fred/series/search"
        method = "GET"
        env_key = "api_key"
        env_value = CANARY_KEY
        provider = "fred"

Usage (public connectors, no key)::

    class TestPolymarketErrorMapping(ErrorMappingSuite):
        connector = POLYMARKET_GAMMA
        params = PolymarketFetchParams(path="/events")
        route_url = "https://gamma-api.polymarket.com/events"
        method = "GET"
        env_key = None
        provider = None  # not asserted when env_key is None
"""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
import pytest
import respx
from parsimony.errors import (
    ConnectorError,
    ProviderError,
    RateLimitError,
)

from parsimony_test_support.harness import (
    CANARY_KEY,
    STATUS_TO_EXC,
    assert_no_secret_leak,
)


class ErrorMappingSuite:
    """Base class for parametric HTTP-error-mapping tests.

    Subclass and override the class attributes. Do NOT add the
    ``@pytest.mark.asyncio`` decorator — inherited methods already carry
    it via pytest-asyncio auto-mode.

    Override :attr:`connector`, :attr:`params`, :attr:`route_url`,
    :attr:`method`. Override :attr:`env_key` to ``None`` for public
    connectors.
    """

    # --- Required overrides ---------------------------------------------
    connector: ClassVar[Any] = None
    params: ClassVar[Any] = None
    route_url: ClassVar[str] = ""
    method: ClassVar[str] = "GET"

    # --- Optional overrides ---------------------------------------------
    env_key: ClassVar[str | None] = "api_key"
    env_value: ClassVar[str] = CANARY_KEY
    provider: ClassVar[str | None] = None

    #: Override when a connector uses a custom mapper that doesn't match
    #: the canonical kernel table (e.g. finnhub maps 403 → PaymentRequired
    #: instead of 402). Drop the entry and add a one-off assertion in the
    #: per-connector test file.
    status_map: ClassVar[list[tuple[int, type[ConnectorError]]]] = STATUS_TO_EXC

    # --- Tests ----------------------------------------------------------

    @respx.mock
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status,exc_type", STATUS_TO_EXC)
    async def test_maps_status_and_does_not_leak_key(
        self, status: int, exc_type: type[ConnectorError]
    ) -> None:
        # Skip parametrize cases this subclass explicitly overrides via
        # ``status_map``. Default is the canonical table → every case runs.
        override = dict(self.status_map)
        if override.get(status) is not exc_type:
            pytest.skip(
                f"{type(self).__name__} overrides {status} mapping"
            )
        route = respx.route(method=self.method, url=self.route_url)
        route.mock(return_value=httpx.Response(status, text=f"status={status}"))

        bound = (
            self.connector.bind(**{self.env_key: self.env_value})
            if self.env_key
            else self.connector
        )
        with pytest.raises(exc_type) as exc_info:
            await bound(self.params)

        assert_no_secret_leak(exc_info.value, secret=self.env_value)
        if self.env_key is not None and self.provider is not None:
            assert exc_info.value.provider == self.provider

    @respx.mock
    @pytest.mark.asyncio
    async def test_rate_limit_carries_retry_after(self) -> None:
        route = respx.route(method=self.method, url=self.route_url)
        route.mock(
            return_value=httpx.Response(429, text="slow", headers={"Retry-After": "17"})
        )

        bound = (
            self.connector.bind(**{self.env_key: self.env_value})
            if self.env_key
            else self.connector
        )
        with pytest.raises(RateLimitError) as exc_info:
            await bound(self.params)
        assert exc_info.value.retry_after == 17.0

    @respx.mock
    @pytest.mark.asyncio
    async def test_provider_error_carries_status_code(self) -> None:
        route = respx.route(method=self.method, url=self.route_url)
        route.mock(return_value=httpx.Response(503, text="unavailable"))

        bound = (
            self.connector.bind(**{self.env_key: self.env_value})
            if self.env_key
            else self.connector
        )
        with pytest.raises(ProviderError) as exc_info:
            await bound(self.params)
        assert exc_info.value.status_code == 503
