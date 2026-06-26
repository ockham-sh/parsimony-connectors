"""Hardened HTML scraping and strict SDMX identifier validation.

The ECB data-portal HTML scrape is the loosest contract we consume;
upstream changes or breaches can flow attacker-shaped strings into
downstream filename construction, ``sdmx1`` calls, and logs. Every
scraped ID must round-trip :func:`validate_sdmx_id` before it is used.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from parsimony_sdmx.core.errors import SdmxFetchError

HTML_PARSER = "lxml"
"""Pinned BeautifulSoup backend — ``lxml`` is deterministic across installs."""

# SDMX identifiers: ASCII letter-led, then letters/digits/underscore/dot/hyphen.
_SDMX_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,199}$")


def parse_html(html: str | bytes) -> BeautifulSoup:
    """Parse ``html`` with the pinned ``lxml`` backend."""
    return BeautifulSoup(html, HTML_PARSER)


def validate_sdmx_id(candidate: str) -> str:
    """Return ``candidate`` unchanged if it matches the SDMX-ID whitelist.

    Rejects empty strings, anything with control characters, spaces,
    slashes, ``$``, quotes, Windows-reserved characters, or lengths
    outside ``[1, 200]``.
    """
    if not isinstance(candidate, str) or not _SDMX_ID_RE.match(candidate):
        raise SdmxFetchError(f"Invalid SDMX identifier from scrape: {candidate!r}")
    return candidate
