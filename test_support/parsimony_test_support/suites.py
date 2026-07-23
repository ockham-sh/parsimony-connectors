"""Reusable pytest suite classes — subclass per connector.

These mixins encode the most-repeated test patterns in the monorepo:

* :class:`ErrorMappingSuite` — canonical HTTP-status → ConnectorError
  mapping plus the Retry-After header contract plus secret-leak defense.
* :class:`CredentialDeclarationSuite` — proves a connector's ``requires=``
  and ``secrets=`` declarations match its runtime behavior.
* :class:`IntegrationSuite` — live-API smoke test wired to
  ``require_env``, skipped when credentials are missing.

Usage (connectors with an API key)::

    from parsimony_fred import fred_search
    from parsimony_test_support import CANARY_KEY
    from parsimony_test_support.suites import ErrorMappingSuite

    class TestFredErrorMapping(ErrorMappingSuite):
        connector = fred_search
        call_kwargs = {"search_text": "x"}
        route_url = "https://api.stlouisfed.org/fred/series/search"
        method = "GET"
        env_key = "api_key"
        env_value = CANARY_KEY
        provider = "fred"

Usage (public connectors, no key)::

    class TestPolymarketErrorMapping(ErrorMappingSuite):
        connector = POLYMARKET_GAMMA
        call_kwargs = {"path": "/events"}
        route_url = "https://gamma-api.polymarket.com/events"
        method = "GET"
        env_key = None
        provider = None  # not asserted when env_key is None
"""

from __future__ import annotations

import contextlib
from typing import Any, ClassVar

import httpx
import pytest
import respx
from parsimony.errors import (
    ConnectorError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)

from parsimony_test_support.harness import (
    CANARY_KEY,
    STATUS_TO_EXC,
    assert_no_secret_leak,
)


class ErrorMappingSuite:
    """Base class for parametric HTTP-error-mapping tests.

    Subclass and override the class attributes. Do NOT add the
    Tests call connectors synchronously — no asyncio marker required.

    Override :attr:`connector`, :attr:`call_kwargs`, :attr:`route_url`,
    :attr:`method`. Override :attr:`env_key` to ``None`` for public
    connectors.
    """

    # --- Required overrides ---------------------------------------------
    connector: ClassVar[Any] = None
    call_kwargs: ClassVar[dict[str, Any]] = {}
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

    def _call(self, connector: Any) -> Any:
        return connector(**self.call_kwargs)

    # --- Tests ----------------------------------------------------------

    @respx.mock
    @pytest.mark.parametrize("status,exc_type", STATUS_TO_EXC)
    def test_maps_status_and_does_not_leak_key(
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
            self._call(bound)

        assert_no_secret_leak(exc_info.value, secret=self.env_value)
        if self.env_key is not None and self.provider is not None:
            assert exc_info.value.provider == self.provider

    @respx.mock
    def test_rate_limit_carries_retry_after(self) -> None:
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
            self._call(bound)
        assert exc_info.value.retry_after == 17.0

    @respx.mock
    def test_provider_error_carries_status_code(self) -> None:
        route = respx.route(method=self.method, url=self.route_url)
        route.mock(return_value=httpx.Response(503, text="unavailable"))

        bound = (
            self.connector.bind(**{self.env_key: self.env_value})
            if self.env_key
            else self.connector
        )
        with pytest.raises(ProviderError) as exc_info:
            self._call(bound)
        assert exc_info.value.status_code == 503


class CredentialDeclarationSuite:
    """Base class proving ``requires=``/``secrets=`` declarations match runtime.

    Contract: a name in ``requires`` is the env var that
    :class:`UnauthorizedError` would name if the connector were called with
    nothing configured. ``secrets`` is the orthogonal list of parameter names
    redacted from provenance. Each test self-guards on the connector's own
    metadata, so the same subclass shape wires onto keyed, optional-key, and
    keyless connectors alike — inapplicable checks skip.

    Override :attr:`connector`, :attr:`call_kwargs`, :attr:`route_url`,
    :attr:`method`.
    """

    # --- Required overrides ---------------------------------------------
    connector: ClassVar[Any] = None
    call_kwargs: ClassVar[dict[str, Any]] = {}
    route_url: ClassVar[str] = ""
    method: ClassVar[str] = "GET"

    def _call(self, connector: Any) -> Any:
        return connector(**self.call_kwargs)

    def _mock_route(self) -> respx.Route:
        route = respx.route(method=self.method, url=self.route_url)
        route.mock(return_value=httpx.Response(200, json={}))
        return route

    @staticmethod
    def _assert_canary_in_request(request: httpx.Request, canary: str) -> None:
        """Assert *canary* appears in the request URL, headers, or body."""
        surfaces = [str(request.url)]
        surfaces.extend(str(value) for value in request.headers.values())
        surfaces.append(request.content.decode("utf-8", errors="replace"))
        assert any(canary in surface for surface in surfaces), (
            f"credential never reached the outgoing request "
            f"(checked URL, headers, and body of {request.method} {request.url})"
        )

    # --- Tests ----------------------------------------------------------

    @respx.mock
    def test_declared_requirement_fast_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With every declared env var absent, the bare call fast-fails pre-network."""
        declared = tuple(self.connector.requires)
        if not declared:
            pytest.skip("connector declares no required env vars")
        for name in declared:
            monkeypatch.delenv(name, raising=False)
        route = self._mock_route()

        with pytest.raises(UnauthorizedError) as exc_info:
            self._call(self.connector)

        named = exc_info.value.env_var
        if len(declared) == 1:
            assert named == declared[0], f"declared requires={declared} but UnauthorizedError names {named!r}"
        else:
            assert named in declared, f"declared requires={declared} but UnauthorizedError names {named!r}"
        assert not route.called, "fast-fail must precede the network call"

    @respx.mock
    def test_undeclared_does_not_fast_fail(self) -> None:
        """With ``requires=()``, the bare call reaches the network — no fast-fail."""
        if self.connector.requires:
            pytest.skip("connector declares required env vars")
        route = self._mock_route()
        try:
            self._call(self.connector)
        except UnauthorizedError:
            pytest.fail(
                f"{self.connector.name}: raised UnauthorizedError while declaring "
                "requires=() — declare the env var in requires="
            )
        except Exception:
            # The minimal mock body may not satisfy the parser; only the
            # request having been issued matters here.
            pass
        assert route.called, "connector never issued the mocked request"

    @respx.mock
    def test_declared_credential_reaches_request(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Env-supplied credentials land in the outgoing request (URL, header, or body)."""
        declared = tuple(self.connector.requires)
        if not declared:
            pytest.skip("connector declares no required env vars")
        for name in declared:
            monkeypatch.setenv(name, CANARY_KEY)
        route = self._mock_route()

        with contextlib.suppress(Exception):
            self._call(self.connector)

        assert route.called, "connector never issued the mocked request"
        self._assert_canary_in_request(route.calls.last.request, CANARY_KEY)

    @respx.mock
    def test_secret_params_reach_request(self) -> None:
        """Each secret-marked parameter, when bound, lands in the outgoing request."""
        secret_params = tuple(self.connector.secrets)
        if not secret_params:
            pytest.skip("connector declares no secret parameters")
        route = self._mock_route()
        calls_seen = 0
        for param in secret_params:
            bound = self.connector.bind(**{param: CANARY_KEY})
            with contextlib.suppress(Exception):
                self._call(bound)
            assert len(route.calls) > calls_seen, f"binding secret {param!r} produced no request"
            calls_seen = len(route.calls)
            self._assert_canary_in_request(route.calls.last.request, CANARY_KEY)
