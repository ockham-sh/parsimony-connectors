"""EODHD transport — keyed client builder and unified error mapping.

Every EODHD connector resolves its client through :func:`_client` (the
canonical §4.3 keyed template: arg → env fallback → fast-fail) and routes its
GET through :func:`eodhd_get` (the package error-mapping chokepoint).

EODHD's status semantics differ from the canonical transport table on two
points, which is why this package drops to a raw ``HttpClient`` plus a
hand-written mapper instead of :func:`parsimony.transport.helpers.fetch_json`
(verified live 2026-06-04):

* an **invalid / missing** key returns **401** (body ``Unauthenticated``), and
* a **plan restriction** (an endpoint or range not in the caller's plan, e.g.
  fundamentals / intraday / macro on a free key) returns **403** (body
  ``Only EOD data allowed for free users``), and
* a **bulk** plan restriction returns **423 Locked** (body
  ``Bulk requests are prohibited for free users``).

The canonical :func:`check_status` table folds 403 into
:class:`UnauthorizedError`; for EODHD a 403 (and a 423) means "your plan does
not grant this," so both map to :class:`PaymentRequiredError`. Because
invalid-key is unambiguously 401, this is a status-only disambiguation (the
finnhub case), not a body-sniffing one (the tiingo dual-403 case), handled by an
``if`` on ``resp.status_code`` *before* :func:`check_status`. The 401 path still
maps to :class:`UnauthorizedError` via the canonical table. Transport failures
(timeout, connection) are mapped inside :meth:`HttpClient.request`.

Auth rides as the ``api_token`` query parameter (alongside EODHD's ``fmt=json``
convention). ``api_token`` is in the transport layer's sensitive-param set, so
it is redacted from every log line and never appears in a request URL surfaced
to the agent. The branch reads only the HTTP status (error bodies are
``text/html`` even on failures) and never a raised exception, so no path can
leak the credential.
"""

from __future__ import annotations

from typing import Any

from parsimony.errors import PaymentRequiredError
from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import make_http_client, require_key

_PROVIDER = "eodhd"
_BASE_URL = "https://eodhd.com/api"
_ENV_VAR = "EODHD_API_KEY"

# EODHD's REST endpoints are not streaming; 15s is a conservative ceiling.
# Bulk endpoints (bulk_eod, exchange_symbols, macro_bulk, fundamentals) override
# this with a longer value via ``timeout=``.
_DEFAULT_TIMEOUT_SECONDS = 15.0

# HTTP statuses EODHD uses for a plan-tier restriction (not a credential
# failure): 403 ("Only EOD data allowed for free users") and 423 Locked
# ("Bulk requests are prohibited for free users"). Both map to
# PaymentRequiredError. Invalid-key is 401, so this is unambiguous on status.
_PLAN_RESTRICTION_STATUSES: frozenset[int] = frozenset({403, 423})


def _client(api_key: str, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the EODHD client.

    Fast-fails with :class:`UnauthorizedError` before any network call when no
    key is available. Auth is the ``api_token`` query parameter (redacted by the
    transport layer), carried alongside EODHD's ``fmt=json`` convention as a
    fixed default param — hence ``make_http_client`` with explicit
    ``query_params`` rather than ``make_api_key_client`` (which can set only the
    key and hardcodes ``apikey``).
    """
    key = require_key(api_key, env_var=_ENV_VAR, provider=_PROVIDER)
    return make_http_client(
        _BASE_URL,
        provider=_PROVIDER,
        query_params={"api_token": key, "fmt": "json"},
        timeout=timeout,
    )


def eodhd_get(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared EODHD GET with EODHD-specific error mapping; returns parsed JSON.

    Drops ``None``-valued params, then maps EODHD's plan-restriction statuses on
    the returned response *before* :func:`check_status`:

    * 403 / 423 → :class:`PaymentRequiredError` (plan restriction — EODHD-specific),
    * everything else → :func:`check_status`'s canonical table (401 →
      Unauthorized, 402 → Payment, 429 → RateLimit, other → Provider). Transport
      failures (timeout, connection) are mapped inside :meth:`HttpClient.request`.

    The branch reads only ``resp.status_code`` — never a raised exception
    carrying the request URL — so the ``api_token`` on the query string cannot
    leak into a traceback.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    resp = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    if resp.status_code in _PLAN_RESTRICTION_STATUSES:
        raise PaymentRequiredError(_PROVIDER, message=f"eodhd plan does not grant access to '{op_name}'")
    check_status(resp, provider=_PROVIDER, op_name=op_name)
    return resp.json()


__all__ = ["_client", "eodhd_get"]
