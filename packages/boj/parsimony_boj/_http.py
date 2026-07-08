"""Bank of Japan (BoJ) Time-Series Data Search transport: constants + clients.

The API is keyless JSON — there is no auth to resolve. Two transport facts are
load-bearing and live here:

* **The ``getDataCode`` limit + ``NEXTPOSITION`` pagination.** A fetch request is
  capped at **250 series codes** and **60,000 data points** ``(series × periods)``
  per response (manual §II.4.(1)). Over the point cap the server returns HTTP 200
  ``"Successfully completed"`` with only the first *K* series and a top-level
  ``NEXTPOSITION`` integer naming the 1-based series position to resume from.
  ``boj_fetch`` paginates on it (re-request with ``startPosition=NEXTPOSITION``)
  so the tail is never silently dropped.
* **Akamai defence.** ``stat-search.boj.or.jp`` sits behind Akamai; every request
  carries a browser ``User-Agent`` and the metadata crawl is throttled (the
  manual cautions against "excessive access").

``boj_fetch`` and ``getMetadata`` both speak GET + JSON, so they go through the
kernel's canonical ``make_http_client`` + ``fetch_json`` pair (GET + status-code
mapping via ``check_status`` + ``json()`` + ``None``-param dropping). BoJ's
status semantics are canonical (400 → bad params, 5xx → server), so there is no
per-package mapper chokepoint.
"""

from __future__ import annotations

from parsimony.transport import HttpClient
from parsimony.transport.helpers import make_http_client
from parsimony_shared.cb_enumerate import MetadataCrawlConfig

PROVIDER = "boj"

BASE_URL = "https://www.stat-search.boj.or.jp/api/v1"

#: Per-call timeout. ``getMetadata`` for the giant DBs is large — ``CO`` (TANKAN)
#: is a ~99 MB / 166k-series response — so allow a long read.
FETCH_TIMEOUT = 120.0

#: BoJ's stat_search endpoints sit behind Akamai, which can block the default
#: httpx User-Agent. A browser UA + a small inter-request delay keep the
#: metadata crawl stable.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

#: Max series codes per ``getDataCode`` request (manual §II.4.(1)). Enforced
#: pre-network in ``_validate_codes``.
MAX_CODES_PER_REQUEST = 250

#: Top-level response tag carrying the next-page cursor when a ``getDataCode``
#: request exceeds the 60,000-point limit. Its presence means the result was
#: truncated at a series boundary and must be resumed via ``startPosition``.
NEXTPOSITION_KEY = "NEXTPOSITION"

#: Safety bound on the ``NEXTPOSITION`` resume loop. With ≤250 codes and ≥1
#: series returned per page, the real loop is short; this only guards against a
#: pathological non-terminating cursor (also covered by a non-advancement check).
MAX_FETCH_PAGES = 300

#: Throttle config for the per-DB ``getMetadata`` fan-out (Akamai-aware).
METADATA_CRAWL = MetadataCrawlConfig(
    inter_request_delay_s=0.5,
    retry_statuses=frozenset({403, 429, 500, 502, 503, 504}),
)


def make_boj_client(timeout: float = FETCH_TIMEOUT) -> HttpClient:
    """Build the canonical keyless BoJ client (GET + JSON via ``fetch_json``)."""
    return make_http_client(BASE_URL, provider=PROVIDER, headers={"User-Agent": BROWSER_USER_AGENT}, timeout=timeout)


__all__ = [
    "BASE_URL",
    "BROWSER_USER_AGENT",
    "FETCH_TIMEOUT",
    "MAX_CODES_PER_REQUEST",
    "MAX_FETCH_PAGES",
    "METADATA_CRAWL",
    "NEXTPOSITION_KEY",
    "PROVIDER",
    "make_boj_client",
]
