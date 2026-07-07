"""Sveriges Riksbank transport — one keyless JSON gateway, five API products.

Every public Riksbank REST API lives behind the same Azure API Management gateway
(``api.riksbank.se``) and returns JSON, so all five product families read through
:func:`parsimony.transport.helpers.fetch_json` (GET + status-code mapping +
``json()`` + ``None``-param dropping). A single client factory differs only in base URL.

The optional ``Ocp-Apim-Subscription-Key`` header rides on every client; it is never
required (all five products are open) and only raises the keyless quota of **5
requests/minute, 1000/day per IP**. It travels in a header (never a query param) so it
stays out of request logs without the transport sensitive-param set.

The five products and their base URLs:

* **SWEA** — interest & exchange rates (``swea/v1``)
* **SWESTR** — Swedish krona short-term rate (``swestr/v1``)
* **Monetary Policy Data** — forecasts & outcomes (``monetary_policy_data/v1``)
* **Holdings** — the Riksbank's securities holdings (``holdings/v1``)
* **Turnover Statistics** — fixed-income & FX market turnover (``turnover-statistics/v1``)
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx
from parsimony.errors import ProviderError
from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import make_http_client

PROVIDER = "riksbank"

SWEA_BASE = "https://api.riksbank.se/swea/v1"
SWESTR_BASE = "https://api.riksbank.se/swestr/v1"
MONETARY_POLICY_BASE = "https://api.riksbank.se/monetary_policy_data/v1"
HOLDINGS_BASE = "https://api.riksbank.se/holdings/v1"
TURNOVER_BASE = "https://api.riksbank.se/turnover-statistics/v1"

_TIMEOUT = 30.0


def _client(base: str, api_key: str = "") -> HttpClient:
    headers: dict[str, str] = {}
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return make_http_client(base, provider=PROVIDER, headers=headers, timeout=_TIMEOUT)


def swea_client(api_key: str = "") -> HttpClient:
    """Keyless SWEA client (interest & exchange rates)."""
    return _client(SWEA_BASE, api_key)


def swestr_client(api_key: str = "") -> HttpClient:
    """Keyless SWESTR client (Swedish krona short-term rate)."""
    return _client(SWESTR_BASE, api_key)


def monetary_policy_client(api_key: str = "") -> HttpClient:
    """Keyless Monetary Policy Data client (forecasts & outcomes)."""
    return _client(MONETARY_POLICY_BASE, api_key)


def holdings_client(api_key: str = "") -> HttpClient:
    """Keyless Holdings client (the Riksbank's securities holdings)."""
    return _client(HOLDINGS_BASE, api_key)


def turnover_client(api_key: str = "") -> HttpClient:
    """Keyless Turnover Statistics client (fixed-income & FX market turnover)."""
    return _client(TURNOVER_BASE, api_key)


def get_json_literal_query(
    base_url: str, query: dict[str, str] | None, *, api_key: str, op_name: str
) -> Any:
    """Raw keyless JSON GET that preserves literal characters in the query string.

    The shared :class:`HttpClient` hands query values to httpx's encoder, which
    percent-encodes the colon in a Monetary Policy ``policy_round_name`` (``2026:1`` ->
    ``2026%3A1``) — and the gateway 404s on the encoded form. So the URL is built with
    ``safe=":"`` and issued directly through a raw ``httpx.Client``, mapping the status
    code through :func:`parsimony.transport.check_status` (the typed-error taxonomy) and
    a timeout to ``ProviderError(status_code=408)``. The optional key rides as a header.
    """
    url = f"{base_url}?{urlencode(query, safe=':')}" if query else base_url
    headers = {"Ocp-Apim-Subscription-Key": api_key} if api_key else {}
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            response = client.get(url, headers=headers)
    except httpx.TimeoutException as exc:
        raise ProviderError(PROVIDER, status_code=408) from exc
    check_status(response, provider=PROVIDER, op_name=op_name)
    return response.json()


__all__ = [
    "PROVIDER",
    "SWEA_BASE",
    "SWESTR_BASE",
    "MONETARY_POLICY_BASE",
    "HOLDINGS_BASE",
    "TURNOVER_BASE",
    "swea_client",
    "swestr_client",
    "monetary_policy_client",
    "holdings_client",
    "turnover_client",
    "get_json_literal_query",
]
