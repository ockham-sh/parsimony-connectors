"""Scrape ECB data-portal dataset descriptions with a disk-backed cache.

Each subprocess starts fresh, so in-memory caching does nothing — the
disk cache keyed by ``(URL, UTC date)`` lets multiple workers in a day
reuse the same HTML without re-scraping. The portal rarely changes
mid-day.

**Cache hardening:** when ``cache_dir`` is omitted, the cache lives in
``$XDG_CACHE_HOME/parsimony-sdmx/ecb-portal`` (defaulting to
``~/.cache/parsimony-sdmx/ecb-portal``). A caller-supplied path is rejected
if it is world-writable. On read, every key is re-validated through
:func:`validate_sdmx_id` and every value is length-capped + stripped of
control characters, so a poisoned cache file cannot inject attacker-
shaped titles into downstream parquet.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import stat
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.html import parse_html, validate_sdmx_id
from parsimony_sdmx.io.http import HttpConfig, bounded_get

logger = logging.getLogger(__name__)

PORTAL_URL = "https://data.ecb.europa.eu/data/datasets"
_DATASET_HREF_RE = re.compile(r"/data/datasets/([A-Z][A-Z0-9_.-]{0,199})$")

# Value hardening: portal descriptions should be short human-readable
# strings. Anything longer is suspicious; control chars are forbidden
# because they can corrupt operator terminals and spoof log lines.
_MAX_VALUE_LEN = 2048
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def default_cache_dir() -> Path:
    """Resolve the default cache directory using XDG conventions."""
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "parsimony-sdmx" / "ecb-portal"


def _ensure_safe_cache_dir(path: Path) -> None:
    """Refuse to use a cache directory whose group/other bits allow write.

    Shared writable directories (``/tmp``, a group-writable home) are
    the classic cache-poisoning vector. Force operators to pick a
    private directory.
    """
    try:
        st = path.stat()
    except FileNotFoundError:
        parent = path.parent
        if parent.exists():
            _ensure_safe_cache_dir(parent)
        return
    if not stat.S_ISDIR(st.st_mode):
        raise SdmxFetchError(f"Cache path {path} exists and is not a directory")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise SdmxFetchError(
            f"Refusing to use world-writable or group-writable cache dir {path}; "
            "pick a user-private directory or unset cache_dir."
        )


def scrape_ecb_portal(
    session: requests.Session,
    cache_dir: Path | None = None,
    url: str = PORTAL_URL,
    today: date | None = None,
    http_config: HttpConfig | None = None,
) -> dict[str, str]:
    """Return ``{dataset_id: description}`` parsed from the ECB portal listing.

    When ``cache_dir`` is ``None``, defaults to ``default_cache_dir()``.
    Set ``cache_dir=Path("/dev/null")`` or similar to disable caching —
    an unwritable target raises :class:`SdmxFetchError`.
    """
    today = today or datetime.now(UTC).date()
    if cache_dir is None:
        cache_dir = default_cache_dir()
    _ensure_safe_cache_dir(cache_dir)

    cached = _read_cache(cache_dir, url, today)
    if cached is not None:
        return cached

    html = bounded_get(session, url, config=http_config)
    descriptions = _parse_portal_listing(html)

    _write_cache(cache_dir, url, today, descriptions)
    return descriptions


def _parse_portal_listing(html: bytes) -> dict[str, str]:
    try:
        soup = parse_html(html)
    except Exception as exc:
        raise SdmxFetchError(f"Failed to parse ECB portal HTML: {exc}") from exc

    result: dict[str, str] = {}
    for item in soup.find_all("div", {"class": "expandable-item"}):
        link = item.find("a", href=_DATASET_HREF_RE)
        if not link:
            continue
        href = link.get("href", "")
        if not isinstance(href, str):
            continue
        match = _DATASET_HREF_RE.search(href)
        if not match:
            continue
        try:
            dataset_id = validate_sdmx_id(match.group(1))
        except SdmxFetchError:
            logger.warning("Skipping unparseable ECB dataset id %r", match.group(1))
            continue
        name = link.get_text(strip=True) or ""
        desc_div = item.find("div", {"class": "expandable-description"})
        description = desc_div.get_text(strip=True) if desc_div else ""
        if name and description:
            combined = f"{name}: {description}"
        elif name:
            combined = name
        elif description:
            combined = description
        else:
            continue
        sanitised = _sanitise_value(combined)
        if sanitised:
            result[dataset_id] = sanitised
    return result


def _sanitise_value(text: str) -> str:
    """Strip control chars, collapse whitespace, cap length."""
    text = _CONTROL_CHARS_RE.sub(" ", text)
    text = " ".join(text.split())
    if len(text) > _MAX_VALUE_LEN:
        text = text[: _MAX_VALUE_LEN - 1].rstrip() + "\u2026"
    return text


def _cache_path(cache_dir: Path, url: str, today: date) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return cache_dir / f"ecb-portal-{today.isoformat()}-{digest}.json"


def _read_cache(cache_dir: Path, url: str, today: date) -> dict[str, str] | None:
    path = _cache_path(cache_dir, url, today)
    if not path.exists():
        return None
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("ECB portal cache %s is unreadable; refetching", path)
        return None
    if not isinstance(raw, dict):
        return None
    cleaned: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, str):
            logger.warning(
                "Ignoring non-string entry in ECB portal cache %s", path
            )
            continue
        try:
            key = validate_sdmx_id(k)
        except SdmxFetchError:
            logger.warning(
                "Ignoring invalid dataset id %r in ECB portal cache %s", k, path
            )
            continue
        sanitised = _sanitise_value(v)
        if sanitised:
            cleaned[key] = sanitised
    return cleaned


def _write_cache(
    cache_dir: Path,
    url: str,
    today: date,
    data: dict[str, str],
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    # Lock down permissions on creation so cache can't be read or
    # overwritten by other users on the same host.
    try:
        os.chmod(cache_dir, 0o700)
    except OSError:
        logger.debug("Could not chmod cache dir %s", cache_dir, exc_info=True)
    path = _cache_path(cache_dir, url, today)
    tmp = path.with_suffix(".tmp")
    try:
        # Use O_NOFOLLOW to refuse writing through a symlink.
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(tmp, flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        tmp.replace(path)
    except OSError:
        logger.warning("Failed to write ECB portal cache %s", path, exc_info=True)
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
