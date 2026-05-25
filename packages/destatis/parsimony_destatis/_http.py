"""Destatis GENESIS-Online HTTP transport."""

from __future__ import annotations

from typing import Any

from parsimony_shared.cb_enumerate import MetadataCrawlConfig, ThrottledJsonFetcher

BASE_URL = "https://www-genesis.destatis.de/genesisGONLINE/api/rest"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Origin": "https://www-genesis.destatis.de",
    "Referer": "https://www-genesis.destatis.de/datenbank/online/",
    "Accept": "application/json",
}

# Concurrency for the per-statistic fan-out. Destatis is happy at 4 parallel
# clients with a 0.25s inter-request delay; bumping concurrency triggers
# 429s and (rarely) 503s.
METADATA_CRAWL = MetadataCrawlConfig(concurrency=4, inter_request_delay_s=0.25)


async def get_path_json(fetcher: ThrottledJsonFetcher, path: str) -> dict[str, Any] | None:
    """GET ``{BASE_URL}{path}`` via the shared throttled fetcher."""
    payload = await fetcher.get_json(f"{BASE_URL}{path}", label=path)
    return payload if isinstance(payload, dict) else None


def looks_like_html(text: str) -> bool:
    """Heuristic: did the new API silently swap us onto the SPA shell?"""
    head = text[:512].lower()
    return "<html" in head or "<!doctype html" in head
