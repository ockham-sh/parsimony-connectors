"""FMP transport — keyed client builder and unified error mapping.

Every FMP connector resolves its client through :func:`_client` (the canonical
§4.3 keyed template: arg → env fallback → fast-fail) and routes its GET through
:func:`fmp_get` (the package error-mapping chokepoint). The screener's
enrichment fan-out reuses :func:`fmp_get` directly.

FMP's status semantics (verified live 2026-06-04) differ from the canonical
transport table on one point, which is why this package drops to a raw
``HttpClient`` plus two plain ``if`` branches on the response status instead of
:func:`parsimony.transport.helpers.fetch_json`:

* an **invalid / missing** key returns **401** (body ``"Invalid API KEY. ..."``)
  → :class:`UnauthorizedError`,
* a **plan / legacy restriction** (an endpoint or tier not in the caller's plan)
  returns **403** (body ``"Legacy Endpoint : ..."`` / plan messages) — and FMP
  also uses **402** for payment — both meaning "your plan does not grant this"
  → :class:`PaymentRequiredError`, and
* the **free-tier rolling quota** *also* returns **403**, but with a
  ``"Limit Reach ..."`` body — a temporary throttle, not an entitlement failure
  → :class:`RateLimitError`, so a caller backs off and retries instead of giving
  up permanently.

The canonical :func:`check_status` table folds 403 into
:class:`UnauthorizedError`; for FMP a 403 (or 402) is never a credential failure
(invalid-key is unambiguously 401), so the status narrows it to "plan or quota"
and the body settles which: a ``"Limit Reach"`` body is the quota throttle,
anything else is a plan restriction. Those two cases are handled by an ``if`` on
``resp.status_code`` *before* :func:`check_status`, which then maps every other
status via the canonical table (401 → Unauthorized, 429 → RateLimit, other →
Provider; FMP premium per-minute limits arrive as 429).

Auth rides as the ``apikey`` query parameter (FMP's convention). ``apikey`` is in
the transport layer's sensitive-param set, so it is redacted from every log line
and never appears in a request URL surfaced to the agent. The branch reads only
``resp.status_code`` and ``resp.text`` (never a raised exception), so no path can
leak the credential into a traceback.
"""

from __future__ import annotations

from typing import Any

from parsimony.errors import PaymentRequiredError, RateLimitError
from parsimony.transport import (
    HttpClient,
    check_status,
    parse_retry_after,
    pooled_client,
)
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
# Invalid-key is unambiguously 401, so on status alone a 402/403 is a plan issue.
_PLAN_RESTRICTION_STATUSES: frozenset[int] = frozenset({402, 403})

# ...but FMP overloads 403 for a *second* thing: the free tier's rolling request
# quota comes back as a 403 whose body reads "Limit Reach ..." (the same string
# FinanceToolkit and others match on). That is a *temporary* throttle, not a
# terminal entitlement failure — telling them apart needs the body, not the
# status. A quota body maps to RateLimitError so a caller retries later instead
# of giving up permanently on transient throttling.
_QUOTA_BODY_MARKER = "limit reach"


def _client(api_key: str, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the FMP client.

    Fast-fails with :class:`UnauthorizedError` before any network call when no
    key is available. Auth is the ``apikey`` query parameter (redacted by the
    transport layer) — FMP's only fixed query param — so ``make_api_key_client``
    (which sets exactly that param) fits.
    """
    key = require_key(api_key, env_var=_ENV_VAR, provider=_PROVIDER)
    return make_api_key_client(
        _BASE_URL, provider=_PROVIDER, api_key=key, api_key_param="apikey", timeout=timeout
    )


def fmp_get(
    http: HttpClient,
    *,
    path: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """Shared FMP GET with FMP-specific error mapping; returns parsed JSON.

    Drops ``None``-valued params, then disambiguates FMP's overloaded plan
    statuses on the returned response *before* :func:`check_status`:

    * 402 / 403 with a "Limit Reach" body → :class:`RateLimitError` (the free
      tier's rolling quota, a temporary throttle),
    * other 402 / 403 → :class:`PaymentRequiredError` (plan / legacy restriction),
    * everything else → :func:`check_status`'s canonical table (401 →
      Unauthorized, 429 → RateLimit, other → Provider). Transport failures
      (timeout, connection) are mapped inside :meth:`HttpClient.request`.

    The branch reads only ``resp.status_code`` / ``resp.text`` — never a raised
    exception carrying the request URL — so the ``apikey`` on the query string
    cannot leak into a traceback.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    resp = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    if resp.status_code in _PLAN_RESTRICTION_STATUSES:
        if _QUOTA_BODY_MARKER in resp.text.lower():
            raise RateLimitError(
                _PROVIDER,
                retry_after=parse_retry_after(resp),
                message=f"fmp free-tier request quota reached on '{op_name}' (rolling limit; retry later)",
            )
        raise PaymentRequiredError(_PROVIDER, message=f"fmp plan does not grant access to '{op_name}'")
    check_status(resp, provider=_PROVIDER, op_name=op_name)
    return resp.json()


__all__ = [
    "_BULK_TIMEOUT_SECONDS",
    "_client",
    "fmp_get",
    "pooled_client",
]
