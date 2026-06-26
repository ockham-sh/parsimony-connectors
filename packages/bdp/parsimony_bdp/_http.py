"""Banco de Portugal (BPstat) transport constants and throttling config.

BPstat is a **keyless** public JSON API (no api_key, no auth header). The base
is a JSON-stat 2.0 hierarchy ``domain → dataset → series``.
"""

from __future__ import annotations

from parsimony_shared.cb_enumerate import MetadataCrawlConfig

BASE_URL = "https://bpstat.bportugal.pt/data/v1"

# BPstat sits behind Akamai. A browser User-Agent plus Origin/Referer headers
# keep both the per-call fetch and the bulk crawl from being challenged.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Accept": "application/json",
    "Origin": "https://bpstat.bportugal.pt",
    "Referer": "https://bpstat.bportugal.pt/",
}

# Conservative throttle for the bulk enumeration crawl. Akamai may answer 403
# under load, so it is in the retry set alongside 429/5xx.
METADATA_CRAWL = MetadataCrawlConfig(
    inter_request_delay_s=0.25,
    retry_statuses=frozenset({403, 429, 500, 502, 503, 504}),
)

# Both list endpoints cap ``page_size`` at 100 (101+ → HTTP 400). For the
# dataset-detail crawl, ``page_size=100`` alone 502s on a big dataset because
# the observation ``value`` array gets too large — pairing it with
# ``obs_last_n=1`` shrinks that array to one point per series, so the 100-series
# page succeeds (~70 KB). The crawl only needs the series ids + labels anyway.
DATASET_PAGE_SIZE = 100
DATASET_CRAWL_PARAMS = {"page_size": DATASET_PAGE_SIZE, "obs_last_n": 1}

# ``/series/?series_ids=`` accepts at most 100 ids per call.
SERIES_BATCH = 100

VALID_LANGS = frozenset({"en", "pt"})

# Defensive page-cap so a runaway ``next_page`` cycle can't exhaust the process.
# The largest real dataset is 16,644 series → 167 pages at page_size=100; 2,000
# pages (200 K series) is far above anything BPstat publishes.
MAX_PAGES_PER_DATASET = 2_000

# Cap descriptions before the embedder sees them (context-window safety).
DESCRIPTION_CHAR_CAP = 1500
