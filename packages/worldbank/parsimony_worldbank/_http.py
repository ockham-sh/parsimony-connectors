"""World Bank API transport constants.

The World Bank API v2 is **keyless** — no api_key, no auth headers. The base URL
serves indicator data, country metadata, and related resources in JSON or XML.
"""

from __future__ import annotations

BASE_URL = "https://api.worldbank.org/v2"

HEADERS = {
    "User-Agent": "parsimony-worldbank/0.0.1",
    "Accept": "application/json",
}

# The API caps ``per_page`` at 100; 100+ returns HTTP 400.
DEFAULT_PAGE_SIZE = 100

# Max pages as a safety valve (100 pages × 100 per_page = 10 000 observations).
MAX_PAGES = 100
