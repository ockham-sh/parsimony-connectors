"""SEC EDGAR transport: hosts, the required User-Agent, clients, shared helpers.

SEC EDGAR is keyless, but its fair-access policy (max 10 requests/second)
*requires* every request to carry a ``User-Agent`` header that identifies the
requester (a name and contact email). A generic or missing User-Agent is
rejected with ``403``/``429`` (verified live: a default library User-Agent gets
an HTTP 403). The header is read from the mandatory ``SEC_EDGAR_USER_AGENT``
environment variable and resolved by :func:`user_agent` before any network call.

The User-Agent is required *infrastructure*, not a secret credential — it is a
header (never a query param, so it is never logged/redacted), so it is
deliberately **not** declared via ``secrets=``/``bind()``/``load()``. There is
no API key.

Three hosts, kept separate because their path spaces do not overlap:

* ``data.sec.gov`` — the submissions + XBRL JSON APIs (it **404s** the
  ``/Archives`` path, so document bodies must go to ``www.sec.gov``).
* ``www.sec.gov``  — the ticker map, ``/Archives`` document bodies, and the
  per-directory ``index.json`` crawl helper.
* ``efts.sec.gov`` — EDGAR full-text search over filing content (2001→present).
"""

from __future__ import annotations

import os
import re

import httpx
from parsimony.errors import InvalidParameterError, UnauthorizedError
from parsimony.transport import HttpClient, map_http_error, map_timeout_error
from parsimony.transport.helpers import make_http_client

PROVIDER = "sec_edgar"
ENV_VAR = "SEC_EDGAR_USER_AGENT"

DATA_BASE = "https://data.sec.gov"
WWW_BASE = "https://www.sec.gov"
EFTS_BASE = "https://efts.sec.gov"

_TIMEOUT = 30.0
# SEC's fair-access page asks clients to declare gzip support; httpx negotiates
# and decodes it automatically, but we send it explicitly to follow the policy.
_EXTRA_HEADERS = {"Accept-Encoding": "gzip, deflate"}


def user_agent() -> str:
    """Resolve the mandatory SEC User-Agent from the environment, or fast-fail.

    SEC rejects generic/missing User-Agents with 403/429, so this raises
    (before any network call) with a clear, actionable error when the operator
    has not set the env var.
    """
    ua = os.environ.get(ENV_VAR, "").strip()
    if not ua:
        raise UnauthorizedError(
            PROVIDER,
            "SEC requires a User-Agent identifying the requester (name + email). "
            f"Set {ENV_VAR} to a string like 'Acme Research contact@acme.com'.",
            env_var=ENV_VAR,
        )
    return ua


def _client(base_url: str) -> HttpClient:
    return make_http_client(
        base_url,
        headers={"User-Agent": user_agent(), **_EXTRA_HEADERS},
        timeout=_TIMEOUT,
    )


def data_client() -> HttpClient:
    """Client for ``data.sec.gov`` (submissions + XBRL JSON APIs)."""
    return _client(DATA_BASE)


def www_client() -> HttpClient:
    """Client for ``www.sec.gov`` (ticker map, ``/Archives`` bodies, ``index.json``)."""
    return _client(WWW_BASE)


def efts_client() -> HttpClient:
    """Client for ``efts.sec.gov`` (full-text search)."""
    return _client(EFTS_BASE)


def normalize_cik(raw: str) -> str:
    """Strip non-digits and zero-pad to the 10-digit CIK the JSON APIs expect."""
    digits = re.sub(r"\D", "", raw or "")
    if not digits:
        raise InvalidParameterError(PROVIDER, "cik must contain digits")
    return digits.zfill(10)


def get_text(http: HttpClient, path: str, *, op_name: str) -> str:
    """GET *path* and return the raw text body (a non-JSON filing document)."""
    try:
        response = http.request("GET", path)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider=PROVIDER, op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider=PROVIDER, op_name=op_name)
    return response.text
