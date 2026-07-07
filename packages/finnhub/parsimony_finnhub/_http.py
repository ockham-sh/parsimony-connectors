"""Finnhub transport — keyed client builder and unified error mapping.

Every Finnhub connector resolves its client through :func:`_client` (the
canonical §4.3 keyed template: arg → env fallback → fast-fail) and routes
its GET through :func:`finnhub_get` (the package error-mapping chokepoint).

Finnhub's status semantics differ from the canonical transport table on a
single point, which is why this package drops to a raw ``HttpClient`` plus a
plain ``if`` on the response status instead of
:func:`parsimony.transport.helpers.fetch_json`:

* an **invalid / missing** key returns **401** (verified live), and
* a **plan restriction** (premium-only endpoint on a free key) returns **403**.

The canonical :func:`check_status` table folds 403 into
:class:`UnauthorizedError`; for finnhub a 403 means "your plan does not grant
this," so it maps to :class:`PaymentRequiredError` via an ``if`` on
``resp.status_code`` *before* :func:`check_status`. The 401 path still maps to
:class:`UnauthorizedError` via the canonical table. Transport failures (timeout,
connection) are mapped inside :meth:`HttpClient.request`.

Auth rides as the ``token`` query parameter. ``token`` is in the transport
layer's sensitive-param set, so it is redacted from every log line and never
appears in a request URL surfaced to the agent. The branch reads only
``resp.status_code`` — never a raised exception — so no path can leak the
credential.
"""

from __future__ import annotations

from typing import Any

from parsimony.errors import PaymentRequiredError
from parsimony.transport import HttpClient, check_status
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
        provider=_PROVIDER,
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

    Drops ``None``-valued params, then maps finnhub's plan-restriction 403 on the
    returned response *before* :func:`check_status`:

    * 403 → :class:`PaymentRequiredError` (plan restriction — finnhub-specific),
    * everything else → :func:`check_status`'s canonical table (401 →
      Unauthorized, 402 → Payment, 429 → RateLimit, other → Provider). Transport
      failures (timeout, connection) are mapped inside :meth:`HttpClient.request`.

    The branch reads only ``resp.status_code`` — never a raised exception
    carrying the request URL — so the ``token`` on the query string cannot leak
    into a traceback.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    resp = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    if resp.status_code == 403:
        raise PaymentRequiredError(_PROVIDER, message=f"finnhub plan does not grant access to '{op_name}'")
    check_status(resp, provider=_PROVIDER, op_name=op_name)
    return resp.json()


__all__ = ["finnhub_get"]
