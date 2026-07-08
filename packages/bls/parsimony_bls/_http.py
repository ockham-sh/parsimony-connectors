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
  Because curl_cffi returns a non-httpx response object, its non-2xx statuses are
  handed to :func:`~parsimony.transport.check_status` (duck-typed on
  ``.status_code`` / ``.headers``); only its timeout / connection failures are
  mapped by hand.
"""

from __future__ import annotations

from typing import Any

from curl_cffi.requests import Session
from curl_cffi.requests import exceptions as curl_exc
from parsimony.errors import ProviderError
from parsimony.transport import HttpClient, check_status

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

    ``fetch_json`` is GET-only, so the POST data path issues the request directly
    and hands the response to :func:`~parsimony.transport.check_status`.
    ``request()`` maps transport failures internally; ``check_status`` raises the
    typed error from any non-2xx status. POST is not retried by the transport
    (non-idempotent), by design. Logical (body-level) failures are the caller's
    responsibility to inspect.
    """
    resp = http.request("POST", path, json=payload, op_name=op_name)
    check_status(resp, provider=PROVIDER, op_name=op_name)
    return resp.json()


# --- curl_cffi bulk download -----------------------------------------------


def make_download_session() -> Session:
    """Build a curl_cffi session for the Akamai-walled download host.

    One session is reused (pooled) across a survey's flat-file fan-out.
    """
    return Session()


def download_text(session: Session, url: str, *, op_name: str) -> str:
    """GET *url* from ``download.bls.gov`` via curl_cffi (Chrome impersonation).

    Raw transport for an Akamai-walled host: issue the GET, inspect
    ``status_code`` directly, hand any non-2xx to
    :func:`~parsimony.transport.check_status`. A curl_cffi timeout / connection
    failure maps to ``ProviderError(408)`` so an agent can fail over.
    """
    try:
        response = session.get(url, impersonate="chrome", timeout=DOWNLOAD_TIMEOUT)
    except curl_exc.Timeout as exc:
        raise ProviderError(PROVIDER, status_code=408) from exc
    except curl_exc.RequestException as exc:
        raise ProviderError(PROVIDER, status_code=408) from exc

    if response.status_code >= 400:
        check_status(response, provider=PROVIDER, op_name=op_name)
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
