"""Alpha Vantage transport — keyed client builder + in-body error detection.

Every Alpha Vantage connector resolves its client through :func:`_client` (the
canonical §4.3 keyed template: arg → ``ALPHA_VANTAGE_API_KEY`` env fallback →
fast-fail with :class:`UnauthorizedError`) and routes its GET through
:func:`av_fetch` (JSON) or :func:`av_fetch_csv` (CSV). Both build on
``parsimony.transport.helpers.fetch_json`` / a thin raw GET, then run the
shared :func:`raise_for_in_body_error` post-parse check.

**The Alpha Vantage quirk (§5.8 "200-with-error-body").** Alpha Vantage returns
**HTTP 200** for logical failures and signals the real failure mode via one of
three top-level keys (verified live 2026-06-04):

* ``"Error Message"`` — a bad query/parameter (unknown function, malformed
  argument). → :class:`ParseError` (it is "200 but not the data shape we
  expected"; carries no falsifiable status).
* ``"Note"`` — the legacy free-tier rate-limit notice. → :class:`RateLimitError`.
* ``"Information"`` — the current catch-all notice. Its text is **byte-identical**
  for both a genuine daily-quota exhaustion AND a premium-endpoint gate:

      "Thank you for using Alpha Vantage! Please consider spreading out your
      free API requests more sparingly (1 request per second). You may subscribe
      to any of the premium plans at https://www.alphavantage.co/premium/ to lift
      the free key rate limit (25 requests per day), raise the per-second burst
      limit, and instantly unlock all premium endpoints"

  Because the body cannot disambiguate the two, this maps to
  :class:`RateLimitError` (``quota_exhausted=True``) — it is literally a quota
  body ("25 requests per day"). A premium-only ``Information`` body that mentions
  premium/subscription WITHOUT any rate-limit language is mapped to
  :class:`PaymentRequiredError` instead (e.g. if Alpha Vantage ever ships a
  distinct premium-gate notice). String-sniffing the body is the sanctioned §5.8
  exception, tolerated only because Alpha Vantage exposes no machine-readable
  error code.

Auth rides as the ``apikey`` query parameter, which is in the transport layer's
sensitive-param set — it is redacted from every log line and never reaches a
surfaced URL or exception message. ``secrets=("api_key",)`` (declared on every
connector) keeps it out of provenance; the two mechanisms are independent.
"""

from __future__ import annotations

import io
import os
from typing import Any

import httpx
import pandas as pd
from parsimony.errors import (
    PaymentRequiredError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import HttpClient, map_http_error, map_timeout_error
from parsimony.transport.helpers import fetch_json, make_http_client

_PROVIDER: str = "alpha_vantage"
_BASE_URL: str = "https://www.alphavantage.co"
_ENV_VAR: str = "ALPHA_VANTAGE_API_KEY"
_DEFAULT_TIMEOUT_SECONDS: float = 20.0
# Alpha Vantage publishes no Retry-After; the free cap resets daily. A 1-hour
# hint is a conservative, non-misleading default (well under the 86400 ceiling).
_RATE_LIMIT_RETRY_AFTER: float = 3600.0

# Tokens that mark the body as a rate-limit / quota notice (vs a pure premium
# gate). Lower-cased substring match (the §5.8 exception — AV has no error code).
_RATE_LIMIT_MARKERS: tuple[str, ...] = (
    "rate limit",
    "per day",
    "per second",
    "per minute",
    "requests more sparingly",
    "frequency",
)
_PREMIUM_MARKERS: tuple[str, ...] = (
    "premium",
    "subscribe",
)


def _client(api_key: str, *, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> HttpClient:
    """Resolve the API key (arg → env fallback) and build the Alpha Vantage client.

    Fast-fails with :class:`UnauthorizedError` before any network call when no
    key is available. The key is sent as the ``apikey`` query parameter (in the
    transport sensitive set, so redacted in logs); ``apikey`` is the only fixed
    query param, so :func:`make_api_key_client`'s default fits — but we use
    :func:`make_http_client` with an explicit ``query_params`` for symmetry with
    the rest of the fleet and to keep the param name explicit.
    """
    key = api_key or os.environ.get(_ENV_VAR, "")
    if not key:
        raise UnauthorizedError(_PROVIDER, env_var=_ENV_VAR)
    return make_http_client(
        _BASE_URL,
        query_params={"apikey": key},
        timeout=timeout,
    )


def raise_for_in_body_error(body: Any, op_name: str) -> None:
    """Detect Alpha Vantage's HTTP-200 in-body error envelopes (§5.8).

    No-op when ``body`` is not a dict or carries none of the marker keys.
    """
    if not isinstance(body, dict):
        return

    if "Error Message" in body:
        # Bad query/parameter — 200 but not the expected data shape.
        from parsimony.errors import ParseError

        raise ParseError(
            _PROVIDER,
            f"alpha_vantage rejected the request for '{op_name}': {body['Error Message']}",
        )

    if "Note" in body:
        raise RateLimitError(
            _PROVIDER,
            retry_after=_RATE_LIMIT_RETRY_AFTER,
            quota_exhausted=True,
            message=f"alpha_vantage rate limit on '{op_name}'",
        )

    if "Information" in body:
        _raise_for_information(str(body["Information"]), op_name)


def _raise_for_information(info: str, op_name: str) -> None:
    """Map an ``Information`` notice body to the right typed error.

    The standard free-tier notice ("25 requests per day", "1 request per second")
    is a quota/threshold body → :class:`RateLimitError`. A premium-only gate that
    mentions premium/subscription WITHOUT rate-limit language →
    :class:`PaymentRequiredError`.
    """
    low = info.lower()
    is_rate_limit = any(marker in low for marker in _RATE_LIMIT_MARKERS)
    is_premium = any(marker in low for marker in _PREMIUM_MARKERS)

    if is_rate_limit:
        raise RateLimitError(
            _PROVIDER,
            retry_after=_RATE_LIMIT_RETRY_AFTER,
            quota_exhausted=True,
            message=f"alpha_vantage rate limit on '{op_name}'",
        )
    if is_premium:
        raise PaymentRequiredError(
            _PROVIDER,
            message=f"alpha_vantage premium endpoint required for '{op_name}'",
        )
    # An unexpected single-key notice we cannot classify — surface as a parse
    # drift rather than guessing.
    from parsimony.errors import ParseError

    raise ParseError(_PROVIDER, f"alpha_vantage returned an unexpected notice for '{op_name}'")


async def av_fetch(
    http: HttpClient,
    *,
    function: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> Any:
    """GET Alpha Vantage's ``/query`` JSON endpoint; return parsed JSON.

    Every JSON endpoint is the same URL (``/query``), differentiated by the
    ``function`` query param. Delegates GET + ``raise_for_status`` + HTTP-error
    mapping + ``None``-param dropping + JSON parse to :func:`fetch_json`, then
    runs the shared in-body error check.
    """
    req_params: dict[str, Any] = {"function": function}
    if params:
        req_params.update(params)
    body = await fetch_json(http, path="query", params=req_params, provider=_PROVIDER, op_name=op_name)
    raise_for_in_body_error(body, op_name)
    return body


async def av_fetch_csv(
    http: HttpClient,
    *,
    function: str,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> pd.DataFrame:
    """Fetch an Alpha Vantage CSV endpoint into a DataFrame.

    CSV endpoints (calendars, listing status) have no JSON helper, so this drops
    to a raw GET (still mapping ``HTTPStatusError`` / ``TimeoutException`` by
    hand per §6.7) and inspects the text for the same in-body error envelopes
    before handing it to pandas.
    """
    req_params: dict[str, Any] = {"function": function}
    if params:
        req_params.update(params)

    try:
        response = await http.request("GET", "/query", params=req_params)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=_PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=_PROVIDER, op_name=op_name)

    text = response.text
    stripped = text.lstrip()
    # CSV endpoints return a JSON notice body (not CSV) on rate-limit / premium.
    if stripped.startswith("{"):
        import json

        try:
            body = json.loads(stripped)
        except json.JSONDecodeError:
            body = None
        if isinstance(body, dict):
            raise_for_in_body_error(body, op_name)
    if stripped.startswith("Information") or stripped.startswith("Note"):
        raise RateLimitError(
            _PROVIDER,
            retry_after=_RATE_LIMIT_RETRY_AFTER,
            quota_exhausted=True,
            message=f"alpha_vantage rate limit on '{op_name}'",
        )

    return pd.read_csv(io.StringIO(text))


def strip_numbered_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Strip Alpha Vantage's numbered key prefixes.

    ``"1. open"`` → ``"open"``, ``"01. symbol"`` → ``"symbol"``.
    """
    return {k.split(". ", 1)[-1] if ". " in k else k: v for k, v in d.items()}


def clean_none_strings(d: dict[str, Any]) -> dict[str, Any]:
    """Replace ``"None"`` string sentinels with ``None`` for proper NaN coercion."""
    return {k: (None if v == "None" else v) for k, v in d.items()}


__all__ = [
    "_client",
    "av_fetch",
    "av_fetch_csv",
    "clean_none_strings",
    "raise_for_in_body_error",
    "strip_numbered_keys",
]
