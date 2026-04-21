"""CoinGecko transport — shared HTTP helpers and unified error mapping.

All 10 ``@connector`` functions route through :func:`coingecko_fetch`. The
enumerator (``enumerate_coingecko``) opens its own pooled ``httpx.AsyncClient``
with a longer 60s timeout because ``/coins/list`` returns ~15k entries; it
sets the same ``x-cg-demo-api-key`` header directly rather than going
through this module's shared client. That is the reason the header string
appears in two places across the package.

Auth rides in a request header (``x-cg-demo-api-key``), not the URL, so URL
redaction is unnecessary — error messages simply omit URL text.
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
from parsimony.result import OutputConfig
from parsimony.http import HttpClient

_DEFAULT_BASE_URL: str = "https://api.coingecko.com/api/v3"
_DEFAULT_TIMEOUT_SECONDS: float = 15.0
_DEFAULT_RATE_LIMIT_RETRY_AFTER: float = 60.0
_PROVIDER: str = "coingecko"

# CoinGecko plan-restriction error codes surfaced inside 401 (and some
# non-standard status) response bodies. Mapped to PaymentRequiredError so
# the caller can distinguish a broken key from a plan-gated endpoint.
_PLAN_RESTRICTION_CODES: frozenset[int] = frozenset({10005, 10006, 10012})


def make_http(api_key: str, base_url: str = _DEFAULT_BASE_URL) -> HttpClient:
    """Construct the standard CoinGecko transport.

    The Demo API key rides as the ``x-cg-demo-api-key`` header (CoinGecko's
    auth convention for the Demo plan). Timeout is 15s — matches the FMP
    precedent and is comfortable for CoinGecko's non-streaming JSON routes.
    """
    return HttpClient(
        base_url,
        headers={"x-cg-demo-api-key": api_key},
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


def _extract_plan_error_code(response: httpx.Response) -> tuple[int, str]:
    """Try to pull CoinGecko's ``(error_code, error_message)`` tuple from a body.

    CoinGecko wraps plan-restriction signals in one of two shapes:

    * ``{"status": {"error_code": N, "error_message": "..."}}``
    * ``{"error": {"status": {"error_code": N, "error_message": "..."}}}``

    Returns ``(0, "")`` if the body is not JSON or the shape is unexpected.
    """
    try:
        body = response.json()
    except ValueError:
        return 0, ""
    if not isinstance(body, dict):
        return 0, ""
    status = body.get("status") or body.get("error", {}).get("status", {})
    if not isinstance(status, dict):
        return 0, ""
    code = status.get("error_code", 0)
    msg = status.get("error_message", "")
    try:
        return int(code), str(msg)
    except (TypeError, ValueError):
        return 0, ""


def _raise_mapped_status(exc: httpx.HTTPStatusError, op_name: str) -> NoReturn:
    """Translate an HTTP error status into a typed connector exception.

    Every HTTP error path funnels through here so the mapping contract is
    uniform: 401/403 → UnauthorizedError, 402 → PaymentRequiredError,
    429 → RateLimitError, else → ProviderError. CoinGecko reuses 401 for
    plan-gated endpoints and for some historical-range restrictions, so the
    401 branch inspects the body's ``error_code`` before deciding.
    """
    status = exc.response.status_code
    match status:
        case 401 | 403:
            code, msg = _extract_plan_error_code(exc.response)
            if code in _PLAN_RESTRICTION_CODES:
                raise PaymentRequiredError(
                    provider=_PROVIDER,
                    message=f"CoinGecko plan restriction (error_code={code}): {msg}",
                ) from exc
            raise UnauthorizedError(
                provider=_PROVIDER,
                message="Invalid or missing CoinGecko API key",
            ) from exc
        case 402:
            raise PaymentRequiredError(
                provider=_PROVIDER,
                message="Your CoinGecko plan does not include this endpoint",
            ) from exc
        case 429:
            retry_after = _parse_retry_after(exc.response)
            raise RateLimitError(
                provider=_PROVIDER,
                retry_after=retry_after,
                message=f"CoinGecko rate limit hit on '{op_name}', retry after {retry_after:.0f}s",
            ) from exc
        case _:
            # Some non-standard status codes still carry plan-restriction bodies.
            code, _msg = _extract_plan_error_code(exc.response)
            if code in _PLAN_RESTRICTION_CODES:
                raise PaymentRequiredError(
                    provider=_PROVIDER,
                    message=f"CoinGecko endpoint requires a higher plan (error_code={code})",
                ) from exc
            raise ProviderError(
                provider=_PROVIDER,
                status_code=status,
                message=f"CoinGecko API error {status} on '{op_name}'",
            ) from exc


async def coingecko_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
    output_config: OutputConfig | None = None,  # noqa: ARG001 — reserved for parity with other packages
) -> Any:
    """Shared CoinGecko GET with typed error mapping.

    Returns the parsed JSON body. Raises typed connector exceptions:
    ``UnauthorizedError``, ``PaymentRequiredError``, ``RateLimitError``,
    or ``ProviderError``. Timeouts become ``ProviderError(status_code=408)``.

    The ``output_config`` argument is accepted for signature parity with
    the other ``*_fetch`` helpers in this monorepo; this connector family
    builds its DataFrames in the caller, so the argument is unused.
    """
    try:
        response = await http.request("GET", path, params=params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_mapped_status(exc, op_name)
    except httpx.TimeoutException as exc:
        raise ProviderError(
            provider=_PROVIDER,
            status_code=408,
            message=f"CoinGecko request timed out on '{op_name}'",
        ) from exc

    return response.json()


__all__ = [
    "coingecko_fetch",
    "make_http",
]
