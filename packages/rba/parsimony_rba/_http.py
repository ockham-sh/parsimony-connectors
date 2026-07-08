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

Because curl_cffi returns a non-httpx response object, :func:`_curl_get` inspects
``response.status_code`` directly and hands any non-2xx to
:func:`~parsimony.transport.check_status`, which raises the typed-error taxonomy
from the status code (429 → :class:`RateLimitError`, 402 →
:class:`PaymentRequiredError`, 401/403 → :class:`UnauthorizedError`, other 4xx/5xx →
:class:`ProviderError`). ``check_status`` is duck-typed — it reads only
``.status_code`` and ``.headers``, both present on a curl_cffi response. curl_cffi
timeout / connection failures are still mapped by hand to
``ProviderError(status_code=408)``.
"""

from __future__ import annotations

import re

from curl_cffi.requests import Session
from curl_cffi.requests import exceptions as curl_exc
from parsimony.errors import ProviderError
from parsimony.transport import check_status

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


def _curl_get(
    session: Session, url: str, *, op_name: str, binary: bool = False
) -> str | bytes:
    """GET *url* via curl_cffi (Chrome impersonation) → text or bytes.

    The raw transport for an Akamai-blocked host: issue the GET, inspect
    ``response.status_code`` directly, and map any non-2xx through
    :func:`~parsimony.transport.check_status`. curl_cffi timeout / connection
    failures map to ``ProviderError(status_code=408)``. The body is parsed
    separately by the caller.
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
        check_status(response, provider="rba", op_name=op_name)

    if binary:
        content = response.content
        return content if isinstance(content, bytes) else bytes(content)
    return str(response.text)


def _make_session() -> Session:
    """Build a curl_cffi sync session. One per ``rba_fetch`` call; reused (pooled) across
    the enumerator's serial crawl."""
    return Session()
