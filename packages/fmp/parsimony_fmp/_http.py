"""FMP transport — keyed client builder and unified error mapping.

Every FMP connector resolves its client through :func:`_client` (the canonical
§4.3 keyed template: arg → env fallback → fast-fail) and routes its GET through
:func:`fmp_get` (the package error-mapping chokepoint). The screener's
enrichment fan-out reuses :func:`fmp_get` directly.

FMP's status semantics (verified live 2026-06-04) differ from the canonical
transport table on one point, which is why this package drops to a raw
``HttpClient`` plus a hand-written mapper instead of
:func:`parsimony.transport.helpers.fetch_json`:

* an **invalid / missing** key returns **401** (body ``"Invalid API KEY. ..."``)
  → :class:`UnauthorizedError`, and
* a **plan / legacy restriction** (an endpoint or tier not in the caller's plan)
  returns **403** (body ``"Legacy Endpoint : ..."`` / plan messages) — and FMP
  also uses **402** for payment — both meaning "your plan does not grant this"
  → :class:`PaymentRequiredError`.

The canonical mapper folds 403 into :class:`UnauthorizedError`; for FMP a 403
(or 402) means a plan restriction, so both map to :class:`PaymentRequiredError`.
Because invalid-key is unambiguously **401**, this is a status-only
disambiguation (the finnhub / eodhd case), not a body-sniffing one (the tiingo
dual-403 case). The 401 path still maps to :class:`UnauthorizedError`. Every
other status flows through the canonical :func:`map_http_error` /
:func:`map_timeout_error`.

Auth rides as the ``apikey`` query parameter (FMP's convention). ``apikey`` is in
the transport layer's sensitive-param set, so it is redacted from every log line
and never appears in a request URL surfaced to the agent. Error bodies are JSON
but are never parsed here — this module branches on the HTTP status alone.
"""

from __future__ import annotations

from typing import Any

import httpx
from parsimony.errors import PaymentRequiredError
from parsimony.transport import HttpClient, map_http_error, map_timeout_error, pooled_client
from parsimony.transport.helpers import make_api_key_client, require_key

_PROVIDER = "fmp"
_BASE_URL = "https://financialmodelingprep.com/stable"
_ENV_VAR = "FMP_API_KEY"

# FMP's REST endpoints are not streaming; 15s is a conservative ceiling.
# Bulk-ish endpoints (full symbol list, market-wide calendars, screener
# enrichment) override this with a longer value via ``timeout=``.
_DEFAULT_TIMEOUT_SECONDS: float = 15.0
_BULK_TIMEOUT_SECONDS: float = 60.0

# HTTP statuses FMP uses for a plan-tier / legacy restriction (not a credential
# failure): 402 (payment required) and 403 ("Legacy Endpoint" / plan messages).
# Both map to PaymentRequiredError. Invalid-key is 401, so this is unambiguous
# on status alone — no body sniffing.
_PLAN_RESTRICTION_STATUSES: frozenset[int] = frozenset({402, 403})


def _client(api_key: str, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the FMP client.

    Fast-fails with :class:`UnauthorizedError` before any network call when no
    key is available. Auth is the ``apikey`` query parameter (redacted by the
    transport layer) — FMP's only fixed query param — so ``make_api_key_client``
    (which sets exactly that param) fits.
    """
    key = require_key(api_key, env_var=_ENV_VAR, provider=_PROVIDER)
    return make_api_key_client(_BASE_URL, api_key=key, api_key_param="apikey", timeout=timeout)


def fmp_get(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared FMP GET with FMP-specific error mapping; returns parsed JSON.

    Drops ``None``-valued params, raises for status, then maps:

    * 402 / 403 → :class:`PaymentRequiredError` (plan / legacy restriction),
    * everything else → :func:`map_http_error` (401 → Unauthorized, 429 →
      RateLimit, other → Provider),
    * timeout → :func:`map_timeout_error` (→ ``ProviderError(408)``).

    Error bodies carry the FMP key only via the (redacted) query string, never in
    the parsed body, so this module never parses an error body.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in _PLAN_RESTRICTION_STATUSES:
            raise PaymentRequiredError(
                _PROVIDER,
                message=f"fmp plan does not grant access to '{op_name}'",
            ) from exc
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)
    return response.json()


__all__ = [
    "_BULK_TIMEOUT_SECONDS",
    "_client",
    "fmp_get",
    "pooled_client",
]
