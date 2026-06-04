"""CoinGecko transport — keyed client builder and unified error mapping.

Every CoinGecko connector resolves its client through :func:`_client` (the
canonical §4.3 keyed template: arg → env fallback → fast-fail) and routes its
GET through :func:`coingecko_fetch` (the package error-mapping chokepoint).

CoinGecko's status semantics differ from the canonical transport table, which
is why this package drops to a raw ``HttpClient`` plus a hand-written mapper
instead of :func:`parsimony.transport.helpers.fetch_json`:

* an **invalid / missing** key returns **401** with ``error_code=10002``, and
* a **plan restriction** (a PRO-only endpoint, or historical data older than
  365 days on the Demo plan) ALSO returns **401**, with ``error_code`` in
  ``{10005, 10006, 10012}`` carried in the response body.

So 401 is genuinely dual-meaning here (verified live, 2026-06-03). A
status-only ``401 → UnauthorizedError`` would mis-diagnose a plan gate as a
broken key, and ``401 → PaymentRequiredError`` would mis-diagnose a typo'd key
as a billing problem. The mapper therefore inspects the body's ``error_code``
before deciding: a plan-restriction code maps to :class:`PaymentRequiredError`;
everything else (including a genuinely bad/absent key) falls through to
:class:`UnauthorizedError`. This is the sanctioned body-disambiguation carve-out
(contract §4.3).

Auth rides in a request header (``x-cg-demo-api-key``), not the URL or a query
param, so the key never reaches a request log line or a surfaced URL — the
transport layer logs only the (redacted) query params and the path, never
headers. No core sensitive-param change is needed (contrast bls's
``registrationkey`` query-param leak).
"""

from __future__ import annotations

import os
from typing import Any, NoReturn

import httpx
from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import HttpClient, map_timeout_error, parse_retry_after
from parsimony.transport.helpers import make_http_client

_PROVIDER = "coingecko"
_BASE_URL = "https://api.coingecko.com/api/v3"
_ENV_VAR = "COINGECKO_API_KEY"

# The /coins/list enumerator returns ~17k rows in one call, so it needs a
# longer ceiling than the per-quote endpoints.
_DEFAULT_TIMEOUT_SECONDS = 15.0

# CoinGecko plan-restriction error codes carried inside a 401 (and, defensively,
# other non-2xx) response body. Distinguishes a plan-gated endpoint / range from
# a genuinely broken key, both of which share HTTP 401:
#   10005 — endpoint is PRO-only (e.g. /coins/top_gainers_losers)
#   10006 — endpoint is Enterprise-only
#   10012 — historical range exceeds the Demo plan's 365-day window
_PLAN_RESTRICTION_CODES: frozenset[int] = frozenset({10005, 10006, 10012})


def _client(api_key: str, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the CoinGecko client.

    Fast-fails with :class:`UnauthorizedError` before any network call when no
    key is available. Auth is the ``x-cg-demo-api-key`` request header, which
    the transport layer never logs, so the key stays out of every log line and
    surfaced URL. The on-chain (GeckoTerminal) routes share this base — they
    are reached via the ``/onchain/...`` path prefix — so one client serves
    every verb.
    """
    key = api_key or os.environ.get(_ENV_VAR, "")
    if not key:
        raise UnauthorizedError(_PROVIDER, env_var=_ENV_VAR)
    return make_http_client(
        _BASE_URL,
        headers={"x-cg-demo-api-key": key},
        timeout=timeout,
    )


def _extract_plan_error_code(response: httpx.Response) -> int:
    """Pull CoinGecko's ``error_code`` from a body, or ``0`` if absent.

    CoinGecko wraps plan-restriction signals in one of two shapes:

    * ``{"status": {"error_code": N, "error_message": "..."}}``
    * ``{"error": {"status": {"error_code": N, "error_message": "..."}}}``

    Returns ``0`` when the body is not JSON or the shape is unexpected. The
    message text is deliberately NOT surfaced — it can embed upstream URLs, and
    branching on the numeric code keeps control flow off strings (§5.6).
    """
    try:
        body = response.json()
    except ValueError:
        return 0
    if not isinstance(body, dict):
        return 0
    status = body.get("status")
    if not isinstance(status, dict):
        error = body.get("error")
        status = error.get("status") if isinstance(error, dict) else None
    if not isinstance(status, dict):
        return 0
    code = status.get("error_code", 0)
    try:
        return int(code)
    except (TypeError, ValueError):
        return 0


def _raise_mapped_status(exc: httpx.HTTPStatusError, op_name: str) -> NoReturn:
    """Translate an HTTP error status into a typed connector exception.

    401 is dual-meaning on CoinGecko (bad key vs plan gate), so the 401 branch
    inspects the body's ``error_code`` before deciding. 402 and any other
    non-2xx that still carries a plan-restriction code map to
    :class:`PaymentRequiredError`; 429 → :class:`RateLimitError`; everything
    else → :class:`ProviderError` with the real status.
    """
    status = exc.response.status_code
    code = _extract_plan_error_code(exc.response)

    if code in _PLAN_RESTRICTION_CODES:
        raise PaymentRequiredError(_PROVIDER, message=f"coingecko plan restriction (error_code={code})") from exc

    match status:
        case 401 | 403:
            # No plan-restriction code → a genuinely invalid / missing key.
            raise UnauthorizedError(_PROVIDER, env_var=_ENV_VAR) from exc
        case 402:
            raise PaymentRequiredError(
                _PROVIDER, message=f"coingecko plan does not grant access to '{op_name}'"
            ) from exc
        case 429:
            retry_after = parse_retry_after(exc.response)
            raise RateLimitError(_PROVIDER, retry_after=retry_after) from exc
        case _:
            raise ProviderError(_PROVIDER, status_code=status) from exc


async def coingecko_fetch(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared CoinGecko GET with coingecko-specific error mapping; returns JSON.

    Drops ``None``-valued params, raises for status, then maps via
    :func:`_raise_mapped_status` (401-body-disambiguation, 402/plan → Payment,
    429 → RateLimit, other → Provider). Timeouts become ``ProviderError(408)``.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = await http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _raise_mapped_status(exc, op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)
    return response.json()


__all__ = ["coingecko_fetch"]
