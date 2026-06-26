"""Destatis GENESIS-Online HTTP transport.

Two transport paths share this module:

* ``destatis_fetch`` (single-table value fetch) uses the canonical
  ``parsimony.transport`` helpers — ``make_http_client`` + ``fetch_json`` —
  so status mapping, timeout mapping, and secret-safe logging come for free.
* ``enumerate_destatis`` (the ``1 + 2N`` metadata fan-out) keeps using the
  shared ``ThrottledJsonFetcher`` (the established catalog-crawl idiom; the
  re-base of ``_shared`` onto core transport is a separate cross-cutting step).

GENESIS-Online is **keyless** — anonymous access needs no credentials. Only a
browser ``User-Agent`` header is sent so the API does not redirect to its SPA
shell.
"""

from __future__ import annotations

from typing import Any

from parsimony.transport import HttpClient
from parsimony_shared.cb_enumerate import MetadataCrawlConfig, ThrottledJsonFetcher

# The legacy ``www-genesis.destatis.de/genesisGONLINE/api/rest`` host now
# 301-redirects to ``genesis.destatis.de/genesis/api/rest`` and doubles the
# path (``/rest/rest/...`` → 404). Point straight at the canonical host so
# every call avoids the redirect round-trip (treasury 301-avoidance lesson).
BASE_URL = "https://genesis.destatis.de/genesis/api/rest"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Accept": "application/json",
}

# Throttling for the per-statistic crawl. Destatis is happy with a serial
# walk at a 0.25s inter-request delay; faster bursts trigger 429s and
# (rarely) 503s.
METADATA_CRAWL = MetadataCrawlConfig(inter_request_delay_s=0.25)


def make_client(timeout: float = 60.0) -> HttpClient:
    """Build the canonical core HTTP client for the single-table fetch path."""
    return HttpClient(BASE_URL, timeout=timeout, headers=HEADERS)


def get_path_json(fetcher: ThrottledJsonFetcher, path: str) -> dict[str, Any] | list[Any] | None:
    """GET ``{BASE_URL}{path}`` via the shared throttled fetcher.

    The ``/statistics`` index returns a JSON **list**; the per-statistic
    ``/information`` and ``/tables`` endpoints return dicts/lists. Return the
    raw decoded payload (dict or list) so the caller can normalise it.
    """
    payload = fetcher.get_json(f"{BASE_URL}{path}", label=path)
    if isinstance(payload, (dict, list)):
        return payload
    return None


def looks_like_html(text: str) -> bool:
    """Heuristic: did the API silently swap us onto the SPA / maintenance shell?"""
    head = text[:512].lower()
    return "<html" in head or "<!doctype html" in head
