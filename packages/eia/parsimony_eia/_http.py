"""US EIA v2 Open Data API transport: key resolution, client, typed-error chokepoint.

The EIA v2 API is a single keyed REST host. The key rides as an ``?api_key=``
query param (``api_key`` is in parsimony-core's sensitive-param set, so it is
auto-redacted from logs) and is also stripped from provenance via
``secrets=("api_key",)`` on every verb.

``eia_get`` is the per-package mapper chokepoint: EIA returns a clean HTTP 400
with a useful JSON body for a bad measure / frequency / facet argument
(``{"error": "Invalid data 'x' ... valid data are 'value'", "code": 400}``).
``check_status`` would collapse that into a generic ``ProviderError(400)`` and
drop the actionable text, so this chokepoint reads the 400 body with a plain
``if`` and raises ``InvalidParameterError`` preserving EIA's message; every other
status falls through to the canonical ``check_status`` table.
"""

from __future__ import annotations

import os
from typing import Any

from parsimony.errors import InvalidParameterError, UnauthorizedError
from parsimony.transport import HttpClient, check_status
from parsimony.transport.helpers import make_http_client

PROVIDER = "eia"
ENV_VAR = "EIA_API_KEY"
BASE_URL = "https://api.eia.gov/v2"

# EIA caps every /data page at 5000 rows: a request with ``length`` > 5000 is
# silently clamped back to 5000 (verified live), so a single call can never
# return more than 5000 of a dataset's ``total`` rows. The fetch connector pages
# through with ``offset`` to reach completeness.
PAGE_SIZE = 5000

# Connectivity for the route-tree enumeration fan-out (~272 nodes). EIA enforces
# a short-window per-second cap and 429s a 6-wide fan-out; 4 keeps the crawl
# under that ceiling (the pooled-client retry policy still absorbs the rare 429)
# so a node is never dropped to a silently-shrunk catalog.
ENUMERATE_CONCURRENCY = 4


def resolve_key(api_key: str) -> str:
    """Resolve the API key (arg → ``EIA_API_KEY`` env fallback) or fast-fail."""
    key = api_key or os.environ.get(ENV_VAR, "")
    if not key:
        raise UnauthorizedError(PROVIDER, env_var=ENV_VAR)
    return key


def make_eia_client(api_key: str, *, timeout: float = 60.0) -> HttpClient:
    """Build the EIA client with the key fixed as a (redacted) default query param."""
    key = resolve_key(api_key)
    return make_http_client(BASE_URL, provider=PROVIDER, query_params={"api_key": key}, timeout=timeout)


def _extract_400_message(response: Any) -> str:
    """Pull EIA's human-readable error string out of a 400 body (bounded length)."""
    try:
        err = response.json().get("error")
    except ValueError:
        err = None
    if isinstance(err, str) and err.strip():
        return err.strip()[:300]
    return "invalid request parameter"


def eia_get(
    http: HttpClient,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    op_name: str,
) -> dict[str, Any]:
    """GET ``{BASE_URL}/{path}`` and return the ``response`` object (dict).

    Drops ``None`` params. ``HttpClient.request`` maps transport failures itself
    and returns the response for any status, so a 400 is inspected here with a
    plain ``if`` and mapped to a message-preserving ``InvalidParameterError``;
    every other non-2xx defers to ``check_status`` (the canonical typed-error
    table). Neither path embeds the query-string key: the 400 message comes from
    the response body and ``check_status`` maps from the status code, not a
    chained exception.
    """
    filtered = {k: v for k, v in (params or {}).items() if v is not None}
    response = http.request("GET", f"/{path.lstrip('/')}", params=filtered or None, op_name=op_name)
    if response.status_code == 400:
        raise InvalidParameterError(PROVIDER, _extract_400_message(response))
    check_status(response, provider=PROVIDER, op_name=op_name)

    body = response.json()
    inner = body.get("response") if isinstance(body, dict) else None
    return inner if isinstance(inner, dict) else {}
