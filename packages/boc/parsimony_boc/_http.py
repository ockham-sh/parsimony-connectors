"""Bank of Canada (BoC) Valet transport: constants + the canonical client.

The Valet API is keyless JSON — there is no auth to resolve, so this module is
thin: a base URL, timeouts, the group-fan-out concurrency cap, and the one
non-obvious transport fact, the **observations request-URI limit**.

``boc_fetch`` and the list endpoints all speak GET + JSON, so they go through
the kernel's canonical ``make_http_client`` + ``fetch_json`` pair (GET +
status→typed-error mapping + ``json()`` + ``None``-param dropping). There is no
provider-specific status semantics to intercept, so there is no per-package
mapper chokepoint here.

Endpoints (base ``https://www.bankofcanada.ca/valet``):

* ``GET /lists/series/json`` (~15.6k) and ``GET /lists/groups/json`` (~2.4k) —
  the catalog index.
* ``GET /groups/{name}/json`` — per-group series membership (the fan-out); a
  retired group returns 404.
* ``GET /observations/{names}/json`` and
  ``GET /observations/group/{name}/json`` — time-series observations.

**Observations request-URI cap (~4096 bytes).** Valet redirects (HTTP 302, to an
error page) any ``/observations`` request whose URL exceeds ~4 KB. The boundary
is on the *URL length*, not the series count: a full URL of 4087 bytes returns
200 while 4127 bytes 302s (measured live 2026-06-09). So a count cap is wrong
(140 short names pass, 140 long names 302). We guard the assembled URL
pre-network and raise a clear :class:`InvalidParameterError` instead of letting
the agent hit an opaque redirect that ``fetch_json`` would surface as a
``ParseError``.
"""

from __future__ import annotations

from parsimony.errors import InvalidParameterError
from parsimony.transport import HttpClient
from parsimony.transport.helpers import make_http_client

PROVIDER = "boc"

BASE_URL = "https://www.bankofcanada.ca/valet"

#: Per-call timeout for both the fetch and the (large) list endpoints. The
#: series index is ~3.5 MB, so allow a long read.
FETCH_TIMEOUT = 60.0

#: Concurrency cap for the per-group fan-out that builds the series→group map.
#: Valet is unauthenticated and tolerates moderate concurrency; 16 keeps the
#: ~2,400-group sweep at ~70 s while staying well under any sensible limit.
GROUP_FETCH_CONCURRENCY = 16

#: Maximum assembled-URL length (host + path, query excluded) we will send to
#: ``/observations``. The server caps the request URI at ~4096 bytes; we sit
#: conservatively below that so the date query string still fits under the cap.
OBSERVATIONS_MAX_URL_BYTES = 4000


def make_valet_client(timeout: float = FETCH_TIMEOUT) -> HttpClient:
    """Build the canonical keyless Valet client (GET + JSON via ``fetch_json``)."""
    return make_http_client(BASE_URL, provider=PROVIDER, timeout=timeout)


def guard_observations_path(path: str, *, series_name: str) -> None:
    """Reject an ``/observations`` path whose URL would exceed the server cap.

    Valet 302-redirects any observations request URL over ~4 KB. Caught
    pre-network, this becomes an actionable :class:`InvalidParameterError`
    (split the request or fetch a whole panel with ``group:NAME``) instead of an
    opaque redirect that downstream parsing would report as a ``ParseError``.
    """
    url_len = len(BASE_URL) + 1 + len(path)
    if url_len > OBSERVATIONS_MAX_URL_BYTES:
        raise InvalidParameterError(
            PROVIDER,
            (
                f"too many/long series names: the request URL is {url_len} bytes but the "
                f"Bank of Canada observations endpoint caps it near {OBSERVATIONS_MAX_URL_BYTES + 96}. "
                "Split the names across multiple boc_fetch calls, or fetch a whole panel in one "
                "call with series_name='group:GROUP_NAME'."
            ),
        )


__all__ = [
    "BASE_URL",
    "FETCH_TIMEOUT",
    "GROUP_FETCH_CONCURRENCY",
    "OBSERVATIONS_MAX_URL_BYTES",
    "PROVIDER",
    "guard_observations_path",
    "make_valet_client",
]
