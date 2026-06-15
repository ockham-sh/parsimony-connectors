"""RBA transport — curl_cffi raw transport + a hand-written error mapper.

The Akamai / ``curl_cffi`` special case
---------------------------------------
``rba.gov.au`` is fronted by Akamai bot-mitigation that **TLS-fingerprint-blocks
stock python-httpx** — every request through the canonical
``make_http_client``/``fetch_json`` path returns HTTP 403. The canonical transport
therefore structurally *cannot reach this host*, which is exactly the §6 sanctioned
"raw transport + custom error mapper" exception. RBA requests go through
**curl_cffi** (``Session(...).get(url, impersonate="chrome")``), which presents a
real Chrome TLS handshake and gets HTTP 200. curl_cffi is a HARD dependency (declared
in ``pyproject.toml``): without it the connector is non-functional.

Because curl_cffi is not httpx, the kernel ``map_http_error`` / ``map_timeout_error``
helpers don't apply. :func:`_curl_get` is the hand-written mapper required by §6: it
inspects ``response.status_code`` and maps to the typed-error taxonomy (429 →
:class:`RateLimitError`, 402 → :class:`PaymentRequiredError`, 401/403 →
:class:`UnauthorizedError`, other 4xx/5xx → :class:`ProviderError`), and converts
curl_cffi timeout/connection failures to ``ProviderError(status_code=408)`` —
mirroring ``map_timeout_error``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from curl_cffi.requests import Session
from curl_cffi.requests import exceptions as curl_exc
from parsimony.errors import (
    PaymentRequiredError,
    ProviderError,
    RateLimitError,
    UnauthorizedError,
)
from parsimony.transport import parse_retry_after

if TYPE_CHECKING:  # pragma: no cover - typing only
    from curl_cffi.requests import Response

_BASE_URL = "https://www.rba.gov.au"
_TABLES_URL = f"{_BASE_URL}/statistics/tables/"
_HISTORICAL_URL = f"{_BASE_URL}/statistics/historical-data.html"

_CSV_LINK_PATTERN = re.compile(r'href="(/statistics/tables/csv/([^"]+)\.csv)"')
_XLSX_LINK_PATTERN = re.compile(r'href="/statistics/tables/xls/([^"]+)\.xlsx"')
_XLS_HIST_LINK_PATTERN = re.compile(r'href="/statistics/tables/xls-hist/([^"]+)\.xls"')

#: Per-request timeout (seconds) for the curl_cffi GETs. The curl_cffi GET
#: impersonates a recent Chrome TLS fingerprint (``impersonate="chrome"``) so
#: Akamai lets the request through; older fingerprints have started to 403.
_TIMEOUT = 60.0

#: Concurrency cap kept for reference — the sync serial crawler no longer needs it,
#: but the constant documents the original rationale (Akamai-fronted unauthenticated
#: host; a bounded serial crawl keeps us under any WAF radar).
_ENUMERATE_CONCURRENCY = 8


def _map_curl_status(status_code: int, *, response: Response, op_name: str) -> None:
    """Map a non-2xx curl_cffi status to the typed-error taxonomy (NoReturn-style).

    Mirrors the kernel ``map_http_error`` status table. RBA is keyless, so 401/403
    are vanishingly unlikely on the data paths (Akamai blocks *before* auth and we
    impersonate a browser), but we map them faithfully for completeness rather than
    letting them fall through to a generic ProviderError.
    """
    if status_code == 429:
        raise RateLimitError("rba", retry_after=_retry_after_seconds(response))
    if status_code == 402:
        raise PaymentRequiredError("rba")
    if status_code in (401, 403):
        raise UnauthorizedError("rba")
    raise ProviderError("rba", status_code=status_code)


def _retry_after_seconds(response: Response, *, default: float = 60.0) -> float:
    """Parse a ``Retry-After`` duration (seconds) from a curl_cffi response.

    The kernel ``parse_retry_after`` is typed for ``httpx.Response`` only; the
    curl_cffi response has a duck-compatible ``headers.get`` but a different static
    type, so we reuse the kernel parser via a tiny shim object that presents just
    the ``.headers`` attribute it reads.
    """
    raw = response.headers.get("Retry-After", "")
    if not str(raw).strip():
        return default

    class _Shim:
        headers = response.headers

    return parse_retry_after(_Shim(), default=default)  # type: ignore[arg-type]


def _curl_get(
    session: Session, url: str, *, op_name: str, binary: bool = False
) -> str | bytes:
    """GET *url* via curl_cffi (Chrome impersonation) → text or bytes.

    The raw §6 transport for an Akamai-blocked host: issue the GET, inspect
    ``response.status_code`` directly, and map any non-2xx through
    :func:`_map_curl_status`. curl_cffi timeout / connection failures map to
    ``ProviderError(status_code=408)`` (the ``map_timeout_error`` convention). The
    body is parsed separately by the caller.
    """
    try:
        response = session.get(url, impersonate="chrome", timeout=_TIMEOUT)
    except curl_exc.Timeout as exc:
        raise ProviderError("rba", status_code=408) from exc
    except curl_exc.RequestException as exc:
        # ConnectionError / DNSError / SSLError / ImpersonateError / etc. — treat any
        # transport-level failure as a transient provider error so the agent can pick
        # another connector (timeout bucket, 408).
        raise ProviderError("rba", status_code=408) from exc

    if response.status_code >= 400:
        _map_curl_status(response.status_code, response=response, op_name=op_name)

    if binary:
        content = response.content
        return content if isinstance(content, bytes) else bytes(content)
    return str(response.text)


def _make_session() -> Session:
    """Build a curl_cffi sync session. One per ``rba_fetch`` call; reused (pooled) across
    the enumerator's serial crawl."""
    return Session()
