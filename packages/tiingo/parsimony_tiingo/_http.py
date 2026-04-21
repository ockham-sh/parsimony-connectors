"""Tiingo transport — shared HTTP helpers and unified error mapping.

Every Tiingo connector in this package routes through the helpers defined
here. That single chokepoint is what guarantees:

- One canonical error mapping (401/403/402/429/other → typed exception).
- ``Retry-After`` header parsing for 429 responses (falls back to 60s).
- No Tiingo API key ever appears in an exception message. Tiingo auth is
  an ``Authorization: Token <key>`` header (not a query-string parameter),
  so URL redaction is unnecessary — the key is never present in
  ``httpx.Request.url``.
"""

from __future__ import annotations

from typing import Any, NoReturn

import httpx
from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import HttpClient
from parsimony.result import OutputConfig

# Per-request timeout. 15s matches the long-standing Tiingo connector
# default; endpoints are REST, not streaming.
_DEFAULT_TIMEOUT_SECONDS: float = 15.0

# Fallback when a 429 response omits ``Retry-After``. RateLimitError requires
# a positive retry_after value; 60s is conservative for an unknown backoff.
_DEFAULT_RATE_LIMIT_RETRY_AFTER: float = 60.0

_DEFAULT_BASE_URL: str = "https://api.tiingo.com"
_PROVIDER: str = "tiingo"


def make_http(api_key: str, base_url: str = _DEFAULT_BASE_URL) -> HttpClient:
    """Construct the standard Tiingo transport.

    All Tiingo connectors use this constructor so that auth, timeouts, and
    header handling are consistent. The API key rides as an
    ``Authorization: Token <key>`` header (Tiingo's auth convention).
    """
    return HttpClient(
        base_url,
        headers={"Authorization": f"Token {api_key}"},
        timeout=_DEFAULT_TIMEOUT_SECONDS,
    )


def _parse_retry_after(response: httpx.Response) -> float:
    """Extract ``Retry-After`` seconds from a 429 response, with a safe default."""
    header = response.headers.get("Retry-After", "").strip()
    if header:
        try:
            value = float(header)
            if 0 < value <= 86_400:
                return value
        except ValueError:
            pass
    return _DEFAULT_RATE_LIMIT_RETRY_AFTER


def _raise_mapped_status(exc: httpx.HTTPStatusError, op_name: str) -> NoReturn:
    """Translate an ``httpx.HTTPStatusError`` into a parsimony-typed exception.

    Every HTTP error path in this package funnels through here so the
    mapping contract is uniform: 401/403 → UnauthorizedError,
    402 → PaymentRequiredError, 429 → RateLimitError,
    else → ProviderError. Messages never echo the API key; the raw
    exception is chained via ``from exc`` for traceback visibility.
    """
    status = exc.response.status_code
    match status:
        case 401 | 403:
            raise UnauthorizedError(
                provider=_PROVIDER,
                message=f"Tiingo endpoint '{op_name}' requires a valid API key or a higher-tier plan",
            ) from exc
        case 402:
            raise PaymentRequiredError(
                provider=_PROVIDER,
                message=f"Tiingo endpoint '{op_name}' requires a higher-tier plan",
            ) from exc
        case 429:
            raise RateLimitError(
                provider=_PROVIDER,
                retry_after=_parse_retry_after(exc.response),
                message=f"Tiingo rate limit hit on '{op_name}'",
            ) from exc
        case _:
            raise ProviderError(
                provider=_PROVIDER,
                status_code=status,
                message=f"Tiingo API error {status} on '{op_name}'",
            ) from exc


async def tiingo_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
    output_config: OutputConfig | None = None,
) -> Any:
    """Shared Tiingo GET with typed error mapping. Returns parsed JSON body.

    ``output_config`` is accepted for signature parity with the other
    connector packages but unused: Tiingo's response shapes are
    heterogenous (lists of dicts, dicts with nested arrays, singletons),
    so each connector does its own row-projection and calls
    ``OutputConfig.build_table_result`` directly.
    """
    del output_config  # reserved for future symmetry; see docstring
    try:
        response = await http.request("GET", path, params=params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_mapped_status(exc, op_name)
    except httpx.TimeoutException as exc:
        raise ProviderError(
            provider=_PROVIDER,
            status_code=408,
            message=f"Tiingo request timed out on '{op_name}'",
        ) from exc

    return response.json()


__all__ = [
    "make_http",
    "tiingo_fetch",
]
