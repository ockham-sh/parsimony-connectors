"""Finnhub transport — shared HTTP helpers and unified error mapping.

Every Finnhub connector in this package routes through the helpers defined
here. That single chokepoint keeps the error-mapping contract uniform
(401 → ``UnauthorizedError``, 403 → ``PaymentRequiredError``, 429 →
``RateLimitError`` with ``Retry-After`` parsing, else → ``ProviderError``)
and keeps timeout handling consistent.

Unlike providers that put the API key in the URL, Finnhub auth rides as
the ``X-Finnhub-Token`` request header. Nothing sensitive appears in the
request URL, so URL redaction is unnecessary.
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
from parsimony.transport import HttpClient, map_timeout_error, parse_retry_after
from parsimony.result import OutputConfig

# Per-request timeout. Finnhub's REST endpoints are not streaming; 15s is a
# conservative ceiling that matches the FMP connector's precedent.
_DEFAULT_TIMEOUT_SECONDS: float = 15.0

_DEFAULT_BASE_URL: str = "https://finnhub.io/api/v1"

_PROVIDER: str = "finnhub"


def make_http(api_key: str, base_url: str = _DEFAULT_BASE_URL) -> HttpClient:
    """Construct the standard Finnhub transport.

    Auth rides as the ``X-Finnhub-Token`` request header (Finnhub's
    convention). Timeout is 15s — provider is not latency-critical.
    """
    return HttpClient(
        base_url,
        headers={"X-Finnhub-Token": api_key},
        timeout=_DEFAULT_TIMEOUT_SECONDS,
    )


def _raise_mapped_status(exc: httpx.HTTPStatusError, op_name: str) -> NoReturn:
    """Translate an HTTP error status into a typed connector exception.

    Every HTTP error path funnels through here so the mapping contract is
    uniform: 401 → ``UnauthorizedError``, 403 → ``PaymentRequiredError``
    (Finnhub returns 403 for free-tier requests against premium
    endpoints), 429 → ``RateLimitError``, else → ``ProviderError``.
    Messages never carry the API key; ``from exc`` chains for traceback
    visibility.
    """
    status = exc.response.status_code
    match status:
        case 401:
            raise UnauthorizedError(
                provider=_PROVIDER,
                message="Invalid or missing Finnhub API key",
            ) from exc
        case 403:
            raise PaymentRequiredError(
                provider=_PROVIDER,
                message=f"Finnhub endpoint '{op_name}' requires a premium plan",
            ) from exc
        case 429:
            raise RateLimitError(
                provider=_PROVIDER,
                retry_after=parse_retry_after(exc.response),
                message=f"Finnhub rate limit hit on '{op_name}' (60 req/min)",
            ) from exc
        case _:
            raise ProviderError(
                provider=_PROVIDER,
                status_code=status,
                message=f"Finnhub API error {status} on '{op_name}'",
            ) from exc


async def finnhub_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
    output_config: OutputConfig | None = None,
) -> Any:
    """Shared Finnhub GET with typed error mapping. Returns parsed JSON body.

    Parameters
    ----------
    http:
        The transport from :func:`make_http`.
    path:
        Finnhub API path (e.g. ``"/quote"``), relative to the base URL.
    params:
        Optional query parameters. ``None`` values are filtered out.
    op_name:
        Connector name used in error messages — e.g. ``"finnhub_quote"``.
    output_config:
        Reserved hook for future consolidation of DataFrame shaping;
        currently unused because the connectors build their own shaped
        DataFrames from the raw JSON.
    """
    del output_config  # Hook retained for future consolidation; unused today.
    try:
        response = await http.request("GET", path, params=params or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_mapped_status(exc, op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)

    return response.json()


__all__ = [
    "finnhub_fetch",
    "make_http",
]
