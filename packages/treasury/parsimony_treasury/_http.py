"""US Treasury transport — keyless httpx across two hosts, two formats.

* **Fiscal Data JSON API** (``api.fiscaldata.treasury.gov``) — read with
  :func:`parsimony.transport.helpers.fetch_json` over :func:`fiscal_client` /
  :func:`metadata_client` (``format=json`` on every data call).
* **Office of Debt Management XML feeds** (``home.treasury.gov``) — OData/Atom XML, so
  they cannot use ``fetch_json`` (GET+JSON only). Read with :func:`get_text` (``request("GET")``
  then :func:`~parsimony.transport.check_status` maps any non-2xx from the response status;
  transport failures are mapped inside ``request()`` itself), then parsed in
  :mod:`parsimony_treasury.parsing`.

Keyless — no API key, no ``secrets=``/``bind``; ``load()`` binds only the catalog URL.
"""

from __future__ import annotations

from typing import Any

from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import make_http_client

PROVIDER = "treasury"

_FISCAL_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
_METADATA_BASE = "https://api.fiscaldata.treasury.gov/services/dtg"

# home.treasury.gov rate feeds: split host from path so the request URL carries no
# trailing slash. The bare ``.../xml/`` form 301-redirects to ``.../xml`` on every call.
_RATES_HOST = "https://home.treasury.gov"
RATES_PATH = "/resource-center/data-chart-center/interest-rates/pages/xml"
RATES_BASE_URL = f"{_RATES_HOST}{RATES_PATH}"

#: Relative path of the dataset metadata endpoint (under the metadata client's base).
METADATA_PATH = "metadata/"

_RATES_TIMEOUT = 30.0


def fiscal_client() -> HttpClient:
    """Keyless Fiscal Data JSON client (``format=json`` on every call)."""
    return make_http_client(_FISCAL_BASE, provider=PROVIDER, query_params={"format": "json"})


def metadata_client() -> HttpClient:
    """Keyless Fiscal Data metadata client (the ``/services/dtg`` base)."""
    return make_http_client(_METADATA_BASE, provider=PROVIDER)


def rates_client() -> HttpClient:
    """Keyless home.treasury.gov XML rate-feed client (targets the host; the canonical
    slash-free feed path is passed per request)."""
    return make_http_client(_RATES_HOST, provider=PROVIDER, timeout=_RATES_TIMEOUT)


def get_text(http: HttpClient, path: str, *, params: dict[str, Any], op_name: str) -> str:
    """GET *path* and return the raw text body (the ODM feeds are XML, not JSON).

    The raw-transport shape for any response ``fetch_json`` can't handle: ``request("GET")``
    then :func:`~parsimony.transport.check_status` maps any non-2xx from the response status.
    """
    response = http.request("GET", path, params=params, op_name=op_name)
    check_status(response, provider=PROVIDER, op_name=op_name)
    return response.text


__all__ = [
    "PROVIDER",
    "RATES_PATH",
    "RATES_BASE_URL",
    "METADATA_PATH",
    "fiscal_client",
    "metadata_client",
    "rates_client",
    "get_text",
]
