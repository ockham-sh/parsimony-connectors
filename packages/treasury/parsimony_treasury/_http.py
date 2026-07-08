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

from parsimony.errors import InvalidParameterError, ParseError
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


def _actionable_400(response: Any) -> str | None:
    """Pull Fiscal Data's human-readable error out of a 400 body (bounded length).

    Prefers ``message`` (which names the offending field/filter) over the less
    specific ``error`` category. Returns ``None`` when the body is not a JSON
    object or carries neither key as a non-empty string.
    """
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    for key in ("message", "error"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:300]
    return None


def fiscal_get(http: HttpClient, path: str, *, params: dict[str, Any], op_name: str) -> Any:
    """GET *path* and return parsed JSON, surfacing Fiscal Data's actionable 400 body.

    Fiscal Data answers a bad field/filter with a clean HTTP 400 whose ``message``
    names the offending parameter ("Invalid query parameter: Field 'X' does not
    exist."). ``check_status`` maps a 400 to an opaque ``ProviderError`` from the
    status alone, dropping that text, so it is read here and raised as a
    message-preserving ``InvalidParameterError``; every other status defers to the
    canonical mapping. Kept local (mirroring the EIA chokepoint) so the framework's
    shared ``fetch_json`` stays provider-agnostic.
    """
    filtered = {k: v for k, v in params.items() if v is not None}
    response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    if response.status_code == 400:
        message = _actionable_400(response)
        if message:
            raise InvalidParameterError(PROVIDER, message)
    check_status(response, provider=PROVIDER, op_name=op_name)
    try:
        return response.json()
    except ValueError as exc:
        raise ParseError(PROVIDER, f"{PROVIDER}: '{op_name}' returned a non-JSON response body") from exc


__all__ = [
    "PROVIDER",
    "RATES_PATH",
    "RATES_BASE_URL",
    "METADATA_PATH",
    "fiscal_client",
    "metadata_client",
    "rates_client",
    "get_text",
    "fiscal_get",
]
