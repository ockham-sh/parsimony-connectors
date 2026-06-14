"""BLS transport: a plain-httpx API path and a curl_cffi bulk-download path.

BLS has two hosts with very different access models:

- **``api.bls.gov``** — the Public Data API v2. Plain JSON over HTTPS; ordinary
  ``httpx`` reaches it. ``bls_fetch`` POSTs here. The optional ``registrationkey``
  only raises quota; logical failure is signalled **in the body** (HTTP 200 + a
  ``status`` field), not via HTTP status codes.
- **``download.bls.gov``** — the bulk flat-file site that carries the authoritative
  per-survey ``.series`` universe. It is **Akamai bot-managed**: a browser
  ``User-Agent`` is not enough, only a real Chrome TLS handshake passes. We reach it
  with ``curl_cffi`` (``impersonate="chrome"``), exactly like ``parsimony-rba``.
  Because curl_cffi is not httpx, its failures are mapped to the typed-error
  taxonomy by hand (the "raw transport + custom mapper" carve-out for a host the
  canonical transport structurally cannot reach).
"""

from __future__ import annotations

from typing import Any

import httpx
from curl_cffi.requests import Response, Session
from curl_cffi.requests import exceptions as curl_exc
from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import HttpClient, map_http_error, map_timeout_error, parse_retry_after

PROVIDER = "bls"

# --- API host (httpx) ------------------------------------------------------

API_BASE = "https://api.bls.gov/publicAPI/v2"
API_TIMEOUT = 60.0

# --- Bulk-download host (curl_cffi) ----------------------------------------

DOWNLOAD_BASE = "https://download.bls.gov/pub/time.series"
DOWNLOAD_TIMEOUT = 120.0

# Defensive cap on a single ``.series`` parse so a runaway / mega-survey file
# can't exhaust memory when a catalog is built on demand. 2,000,000 rows is far
# above any headline survey (the largest headline ``.series`` is ~100k rows);
# the GB-scale microdata surveys exceed it and are caught with a clear error.
DEFAULT_MAX_SERIES_ROWS = 2_000_000


def post_api_json(
    http: HttpClient, path: str, payload: dict[str, Any], *, op_name: str
) -> Any:
    """POST a JSON body to the BLS API and return parsed JSON.

    ``fetch_json`` is GET-only, so the POST data path uses this:
    ``raise_for_status()`` + the kernel ``map_http_error`` / ``map_timeout_error``.
    POST is not retried by the transport (non-idempotent), by design. Logical
    (body-level) failures are the caller's responsibility to inspect.
    """
    try:
        response = http.request("POST", path, json=payload)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=PROVIDER, op_name=op_name)
    return response.json()


# --- curl_cffi bulk download -----------------------------------------------


def _map_curl_status(status_code: int, *, response: Response) -> None:
    """Map a non-2xx curl_cffi status to the typed-error taxonomy (NoReturn-style)."""
    if status_code == 429:
        raise RateLimitError(PROVIDER, retry_after=_retry_after_seconds(response))
    if status_code == 402:
        raise PaymentRequiredError(PROVIDER)
    if status_code in (401, 403):
        # Akamai "Access Denied" surfaces as 200-with-HTML far more often than a
        # real 403, but map faithfully if the edge ever returns one.
        raise UnauthorizedError(PROVIDER)
    raise ProviderError(PROVIDER, status_code=status_code)


def _retry_after_seconds(response: Response, *, default: float = 60.0) -> float:
    raw = response.headers.get("Retry-After", "")
    if not str(raw).strip():
        return default

    class _Shim:
        headers = response.headers

    return parse_retry_after(_Shim(), default=default)  # type: ignore[arg-type]


def make_download_session() -> Session:
    """Build a curl_cffi session for the Akamai-walled download host.

    One session is reused (pooled) across a survey's flat-file fan-out.
    """
    return Session()


def download_text(session: Session, url: str, *, op_name: str) -> str:
    """GET *url* from ``download.bls.gov`` via curl_cffi (Chrome impersonation).

    Raw transport for an Akamai-walled host: issue the GET, inspect
    ``status_code`` directly, map any non-2xx through :func:`_map_curl_status`.
    A curl_cffi timeout / connection failure maps to ``ProviderError(408)``
    (the ``map_timeout_error`` convention) so an agent can fail over.
    """
    try:
        response = session.get(url, impersonate="chrome", timeout=DOWNLOAD_TIMEOUT)
    except curl_exc.Timeout as exc:
        raise ProviderError(PROVIDER, status_code=408) from exc
    except curl_exc.RequestException as exc:
        raise ProviderError(PROVIDER, status_code=408) from exc

    if response.status_code >= 400:
        _map_curl_status(response.status_code, response=response)
    return str(response.text)


__all__ = [
    "API_BASE",
    "API_TIMEOUT",
    "DEFAULT_MAX_SERIES_ROWS",
    "DOWNLOAD_BASE",
    "DOWNLOAD_TIMEOUT",
    "PROVIDER",
    "download_text",
    "make_download_session",
    "post_api_json",
]
