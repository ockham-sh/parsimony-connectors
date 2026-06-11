"""Error-mapping contract for parsimony-rba (Reserve Bank of Australia).

RBA's transport is **curl_cffi**, not httpx — the canonical ``ErrorMappingSuite``
(which mocks httpx via respx) does not apply. The Akamai-blocked host means the
kernel ``map_http_error`` / ``map_timeout_error`` helpers can't be used either;
``_curl_get`` carries a hand-written mapper (the §6 "raw + custom mapper"
exception). These tests pin that mapper directly: HTTP status → typed error, the
Retry-After contract, and curl_cffi timeout/connection failures → ProviderError(408).
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from curl_cffi.requests import exceptions as curl_exc
from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)

import parsimony_rba as pkg


class _FakeResponse:
    def __init__(self, status_code: int, *, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.text = f"status={status_code}"
        self.content = self.text.encode("utf-8")
        self.headers = headers or {}


class _ErrSession:
    """A fake curl_cffi session whose ``get`` returns a status, or raises."""

    def __init__(
        self, *, status: int | None = None, raises: BaseException | None = None, headers: dict[str, str] | None = None
    ) -> None:
        self._status = status
        self._raises = raises
        self._headers = headers

    def get(self, url: str, *, impersonate: str = "chrome", timeout: float = 60.0) -> _FakeResponse:
        if self._raises is not None:
            raise self._raises
        assert self._status is not None
        return _FakeResponse(self._status, headers=self._headers)


@pytest.mark.parametrize(
    "status,exc_type",
    [
        (401, UnauthorizedError),
        (403, UnauthorizedError),
        (402, PaymentRequiredError),
        (429, RateLimitError),
        (404, ProviderError),
        (500, ProviderError),
        (503, ProviderError),
    ],
)
def test_curl_get_maps_status_to_typed_error(status: int, exc_type: type) -> None:
    session = _ErrSession(status=status)
    with pytest.raises(exc_type):
        pkg._curl_get(cast(Any, session), "https://www.rba.gov.au/x", op_name="test")


def test_curl_get_provider_error_carries_status_code() -> None:
    session = _ErrSession(status=503)
    with pytest.raises(ProviderError) as exc_info:
        pkg._curl_get(cast(Any, session), "https://www.rba.gov.au/x", op_name="test")
    assert exc_info.value.status_code == 503
    assert exc_info.value.provider == "rba"


def test_curl_get_rate_limit_carries_retry_after() -> None:
    session = _ErrSession(status=429, headers={"Retry-After": "17"})
    with pytest.raises(RateLimitError) as exc_info:
        pkg._curl_get(cast(Any, session), "https://www.rba.gov.au/x", op_name="test")
    assert exc_info.value.retry_after == 17.0


def test_curl_get_rate_limit_defaults_retry_after_without_header() -> None:
    """No Retry-After header → the kernel default (no crash)."""
    session = _ErrSession(status=429)
    with pytest.raises(RateLimitError) as exc_info:
        pkg._curl_get(cast(Any, session), "https://www.rba.gov.au/x", op_name="test")
    assert exc_info.value.retry_after == 60.0


def test_curl_get_timeout_maps_to_provider_error_408() -> None:
    """A curl_cffi timeout maps to ProviderError(status_code=408) — mirrors
    the kernel ``map_timeout_error`` convention."""
    session = _ErrSession(raises=curl_exc.Timeout("timed out"))
    with pytest.raises(ProviderError) as exc_info:
        pkg._curl_get(cast(Any, session), "https://www.rba.gov.au/x", op_name="test")
    assert exc_info.value.status_code == 408


def test_curl_get_connection_error_maps_to_provider_error_408() -> None:
    """Any curl_cffi transport failure (ConnectionError / DNSError / SSLError /
    ImpersonateError, all RequestException subclasses) → ProviderError(408)."""
    session = _ErrSession(raises=curl_exc.ConnectionError("no route"))
    with pytest.raises(ProviderError) as exc_info:
        pkg._curl_get(cast(Any, session), "https://www.rba.gov.au/x", op_name="test")
    assert exc_info.value.status_code == 408


def test_curl_get_returns_text_on_200() -> None:
    class _OkSession:
        def get(self, url: str, *, impersonate: str = "chrome", timeout: float = 60.0) -> _FakeResponse:
            r = _FakeResponse(200)
            r.text = "hello body"
            return r

    body = pkg._curl_get(cast(Any, _OkSession()), "https://www.rba.gov.au/x", op_name="test")
    assert body == "hello body"


def test_curl_get_returns_bytes_on_200_binary() -> None:
    class _OkSession:
        def get(self, url: str, *, impersonate: str = "chrome", timeout: float = 60.0) -> _FakeResponse:
            r = _FakeResponse(200)
            r.content = b"\x50\x4b\x03\x04xlsx-bytes"
            return r

    data = pkg._curl_get(cast(Any, _OkSession()), "https://www.rba.gov.au/x.xlsx", op_name="test", binary=True)
    assert isinstance(data, bytes)
    assert data.startswith(b"\x50\x4b")
