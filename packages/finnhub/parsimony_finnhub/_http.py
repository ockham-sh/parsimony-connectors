"""Finnhub transport — keyed client builder and unified error mapping.

Every Finnhub connector resolves its client through :func:`_client` (the
canonical §4.3 keyed template: arg → env fallback → fast-fail) and routes
its GET through :func:`finnhub_get` (the package error-mapping chokepoint).

Finnhub's status semantics differ from the canonical transport table on a
single point, which is why this package drops to a raw ``HttpClient`` plus a
hand-written mapper instead of :func:`parsimony.transport.helpers.fetch_json`:

* an **invalid / missing** key returns **401** (verified live), and
* a **plan restriction** (premium-only endpoint on a free key) returns **403**.

The canonical mapper folds 403 into :class:`UnauthorizedError`; for finnhub a
403 means "your plan does not grant this," so it maps to
:class:`PaymentRequiredError`. The 401 path still maps to
:class:`UnauthorizedError`. Every other status flows through the canonical
:func:`map_http_error` / :func:`map_timeout_error`.

Auth rides as the ``token`` query parameter. ``token`` is in the transport
layer's sensitive-param set, so it is redacted from every log line and never
appears in a request URL surfaced to the agent.
"""

from __future__ import annotations

from typing import Any

import httpx
from parsimony.errors import PaymentRequiredError
from parsimony.transport import HttpClient, map_http_error, map_timeout_error
from parsimony.transport.helpers import make_api_key_client, require_key

_PROVIDER = "finnhub"
_BASE_URL = "https://finnhub.io/api/v1"
_ENV_VAR = "FINNHUB_API_KEY"
_API_KEY_PARAM = "token"

# Finnhub's REST endpoints are not streaming; 15s is a conservative ceiling.
# The /stock/symbol enumerator 302-redirects to a multi-MB CDN file, so it
# overrides this with a longer timeout via ``timeout=``.
_DEFAULT_TIMEOUT_SECONDS = 15.0


def _client(api_key: str, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the Finnhub client.

    Fast-fails with :class:`UnauthorizedError` before any network call when no
    key is available. Auth is the ``token`` query parameter (redacted by the
    transport layer). ``HttpClient`` follows redirects by default, which the
    enumerator's CDN 302 relies on.
    """
    key = require_key(api_key, env_var=_ENV_VAR, provider=_PROVIDER)
    return make_api_key_client(
        _BASE_URL,
        api_key=key,
        api_key_param=_API_KEY_PARAM,
        timeout=timeout,
    )


def finnhub_get(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared Finnhub GET with finnhub-specific error mapping; returns JSON.

    Drops ``None``-valued params, raises for status, then maps:

    * 403 → :class:`PaymentRequiredError` (plan restriction — finnhub-specific),
    * everything else → :func:`map_http_error` (401 → Unauthorized, 402 →
      Payment, 429 → RateLimit, other → Provider),
    * timeout → :func:`map_timeout_error` (→ ``ProviderError(408)``).
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            raise PaymentRequiredError(
                _PROVIDER,
                message=f"finnhub plan does not grant access to '{op_name}'",
            ) from exc
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)
    return response.json()


__all__ = ["finnhub_get"]
