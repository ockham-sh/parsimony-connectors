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

from typing import Any

import httpx
from parsimony.transport import HttpClient, map_http_error, map_timeout_error
from parsimony.result import OutputConfig

# Per-request timeout. 15s matches the long-standing Tiingo connector
# default; endpoints are REST, not streaming.
_DEFAULT_TIMEOUT_SECONDS: float = 15.0

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
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)

    return response.json()


__all__ = [
    "make_http",
    "tiingo_fetch",
]
