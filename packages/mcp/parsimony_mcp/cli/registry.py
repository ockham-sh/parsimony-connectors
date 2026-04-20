"""Registry fetch + cache + offline-fallback layer.

Why this layer is careful despite being "just a JSON GET":

* The init wizard runs on a developer's machine and will act on the
  registry's contents — prompting for API keys, installing packages,
  writing config files. A malformed, substituted, or mis-served
  registry cascades into user-visible damage. Validate strictly at
  the boundary (Collina Principle 3) and fail with actionable prose.
* The URL is configurable via ``--registry URL``. Blindly trusting
  that override invites SSRF: an attacker-supplied URL like
  ``https://169.254.169.254/…`` would exfiltrate the CI runner's
  cloud-metadata creds. The allow-list rejects non-HTTPS, userinfo,
  and hosts resolving to private / loopback / link-local / carrier
  NAT ranges (Hunt Principle 6 — make code transparent to analysis).
* First-run on a fresh laptop will always hit the network; steady
  state should not. 24-hour mtime cache + fall-through to stale on
  fetch failure keeps the wizard usable on a plane.
* No retry storm: ONE retry on transient errors only (connect /
  read-timeout / 5xx) with jittered backoff. 4xx is a programmer
  error (we built the wrong URL); retrying doesn't help.

The ``httpx.Client`` is injectable so tests back it with
``httpx.MockTransport`` via the project's respx dev-dep rather than
reaching for per-test monkey-patches.
"""

from __future__ import annotations

import contextlib
import ipaddress
import logging
import os
import random
import socket
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

import httpx
from pydantic import ValidationError

from parsimony_mcp.cli.registry_schema import SCHEMA_VERSION, Registry

_LOG = logging.getLogger("parsimony_mcp.cli.registry")

DEFAULT_REGISTRY_URL: Final[str] = (
    "https://raw.githubusercontent.com/ockham-sh/parsimony-connectors/main/registry.json"
)
DEFAULT_CACHE_PATH: Final[Path] = Path.home() / ".cache" / "parsimony-mcp" / "registry.json"
CACHE_TTL_SECONDS: Final[int] = 24 * 60 * 60

_CONNECT_TIMEOUT: Final[float] = 5.0
_READ_TIMEOUT: Final[float] = 10.0
_RETRY_BACKOFF_BASE_SECONDS: Final[float] = 0.5
_RETRY_BACKOFF_JITTER_SECONDS: Final[float] = 0.5
_MAX_RESPONSE_BYTES: Final[int] = 2 * 1024 * 1024  # 2 MiB — registry.json is < 20 KiB today


class RegistryError(Exception):
    """Registry cannot be obtained, validated, or is otherwise unusable.

    Subclasses differentiate the three failure modes Task 12
    distinguishes in user prose: DNS (no network), upstream
    (reachable but broken), and malformed (parsed response is not
    a valid registry). The base class is kept for ``except
    RegistryError`` at the CLI edge.
    """


class RegistryDNSError(RegistryError):
    """Hostname did not resolve — the machine appears offline."""


class RegistryUpstreamError(RegistryError):
    """Host resolved but returned an error or refused the connection.

    ``status`` is the HTTP status when the server answered (None
    for transport-layer failures that never saw a response).
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class RegistryMalformedError(RegistryError):
    """Response parsed at the network layer but failed schema validation."""


class RegistrySchemaMismatchError(RegistryError):
    """Registry schema version is newer than this client understands."""


@dataclass(frozen=True)
class RegistrySource:
    """Where the returned registry came from, and how fresh it is.

    Used by the wizard's summary line ("fetched from network",
    "using cached registry from 3 hours ago", etc.) and by tests.
    """

    origin: str  # "network" | "cache-fresh" | "cache-stale"
    url: str
    cache_path: Path | None
    cache_age_seconds: float | None


def fetch_registry(
    *,
    url: str = DEFAULT_REGISTRY_URL,
    cache_path: Path | None = DEFAULT_CACHE_PATH,
    client: httpx.Client | None = None,
    ttl_seconds: int = CACHE_TTL_SECONDS,
    now: float | None = None,
) -> tuple[Registry, RegistrySource]:
    """Return a validated :class:`Registry` and metadata about its source.

    Precedence when ``cache_path`` is provided (default flow):

    1. Fresh disk cache (mtime within ``ttl_seconds``) → return as-is.
    2. Network fetch succeeds → validate, atomically replace cache, return.
    3. Network fetch fails → WARN and return stale cache if present.
    4. Both unavailable → :exc:`RegistryError`.

    When ``cache_path`` is ``None`` (custom-URL override), the cache
    is neither read nor written: the caller owns freshness. A single
    fetch attempt (still retried once on transient error) is made,
    validated, and returned, or the function raises.

    ``client`` is injected for testability; if ``None``, a short-lived
    default client is created and closed inside this function.
    """
    now_ts = now if now is not None else time.time()

    if cache_path is not None:
        cached = _read_cache(cache_path)
        if cached is not None:
            registry, cache_mtime = cached
            age = now_ts - cache_mtime
            if age <= ttl_seconds:
                return registry, RegistrySource(
                    origin="cache-fresh",
                    url=url,
                    cache_path=cache_path,
                    cache_age_seconds=age,
                )

    _validate_url(url)

    try:
        raw = _fetch_with_retry(url, client=client)
        registry = _parse_registry(raw, url=url)
    except RegistryError:
        # Network-side failure is operational (Collina P1). If we have
        # any cache at all, surface the stale one — the wizard can
        # still run. If not, the caller's error branch handles it.
        if cache_path is not None:
            stale = _read_cache(cache_path)
            if stale is not None:
                stale_registry, stale_mtime = stale
                _LOG.warning(
                    "registry fetch failed; using stale cache",
                    extra={"url": url, "cache_age_seconds": now_ts - stale_mtime},
                )
                return stale_registry, RegistrySource(
                    origin="cache-stale",
                    url=url,
                    cache_path=cache_path,
                    cache_age_seconds=now_ts - stale_mtime,
                )
        raise

    if cache_path is not None:
        _write_cache(cache_path, raw)

    return registry, RegistrySource(
        origin="network",
        url=url,
        cache_path=cache_path,
        cache_age_seconds=None,
    )


# --------------------------------------------------------------------- URL validation


def _validate_url(url: str) -> None:
    """Reject URLs that could exfiltrate internal traffic (SSRF guard).

    Rules:

    * Scheme must be ``https`` — ``file://``, ``http://``,
      ``gopher://`` and friends cannot reach the registry.
    * No userinfo (``https://user:pass@…``) — we never send creds.
    * Host must resolve only to globally-routable IPs. ``is_global``
      in :mod:`ipaddress` rejects private RFC1918, loopback,
      link-local (including the 169.254.169.254 cloud-metadata
      magic address), multicast, and reserved ranges.
    """
    try:
        parsed = urlparse(url)
    except ValueError as exc:
        raise RegistryError(f"invalid registry URL: {exc}") from exc

    if parsed.scheme != "https":
        raise RegistryError(
            f"registry URL must use https:// scheme; got {parsed.scheme!r} in {url!r}"
        )
    if parsed.username or parsed.password:
        raise RegistryError("registry URL must not contain userinfo (user:pass@host)")
    if not parsed.hostname:
        raise RegistryError(f"registry URL has no hostname: {url!r}")

    try:
        # getaddrinfo is the same resolver httpx ends up using; resolve
        # once here so an attacker can't DNS-rebind between check and
        # fetch. The resolved IPs flow into the httpx request via the
        # transport in production; for tests, MockTransport bypasses
        # this code path by design.
        infos = socket.getaddrinfo(parsed.hostname, None)
    except socket.gaierror as exc:
        raise RegistryDNSError(
            f"could not resolve registry host {parsed.hostname!r}: {exc}"
        ) from exc

    for info in infos:
        sockaddr = info[4]
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if not ip.is_global:
            raise RegistryError(
                f"registry host {parsed.hostname!r} resolves to non-global address {ip_str} — "
                f"refusing to fetch (SSRF guard). Allowed only for public HTTPS hosts."
            )


# --------------------------------------------------------------------- fetch


def _fetch_with_retry(url: str, *, client: httpx.Client | None) -> bytes:
    """Fetch ``url`` with at most one retry on transient errors.

    Transient: connect error, read timeout, 5xx response. 4xx is a
    deterministic configuration bug — we built the wrong URL —
    retrying wastes time.
    """
    owns_client = client is None
    c = client or httpx.Client(
        timeout=httpx.Timeout(connect=_CONNECT_TIMEOUT, read=_READ_TIMEOUT, write=5.0, pool=5.0),
        follow_redirects=True,
    )
    try:
        last_error: Exception | None = None
        for attempt in (1, 2):
            try:
                response = c.get(url)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
                last_error = exc
                if attempt == 1:
                    _sleep_with_jitter()
                    continue
                raise RegistryUpstreamError(
                    f"registry fetch network error: {exc}"
                ) from exc

            if 500 <= response.status_code < 600:
                last_error = RegistryUpstreamError(
                    f"registry server error HTTP {response.status_code} from {url}",
                    status=response.status_code,
                )
                if attempt == 1:
                    _sleep_with_jitter()
                    continue
                raise last_error
            if response.status_code >= 400:
                raise RegistryUpstreamError(
                    f"registry fetch failed: HTTP {response.status_code} from {url} "
                    f"(client error — check --registry URL)",
                    status=response.status_code,
                )

            content = response.content
            if len(content) > _MAX_RESPONSE_BYTES:
                raise RegistryError(
                    f"registry response is {len(content)} bytes, exceeds "
                    f"{_MAX_RESPONSE_BYTES}-byte safety cap"
                )
            return content
        # Unreachable: both attempts either returned or raised.
        assert last_error is not None
        raise RegistryError(str(last_error)) from last_error
    finally:
        if owns_client:
            c.close()


def _sleep_with_jitter() -> None:
    """Back off ``_RETRY_BACKOFF_BASE_SECONDS`` + jitter before retrying.

    Jitter is deliberate: without it, N init wizards launched at the
    same moment against a flaky registry would retry in lockstep.
    """
    delay = _RETRY_BACKOFF_BASE_SECONDS + random.uniform(0, _RETRY_BACKOFF_JITTER_SECONDS)  # noqa: S311 — jitter, not crypto
    time.sleep(delay)


# --------------------------------------------------------------------- validation


def _parse_registry(raw: bytes, *, url: str) -> Registry:
    """Parse ``raw`` as UTF-8 JSON and validate through the shared schema."""
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RegistryError(f"registry payload from {url} is not valid UTF-8: {exc}") from exc

    try:
        return Registry.model_validate_json(decoded)
    except ValidationError as exc:
        # Detect the specific schema-version mismatch up front so the
        # error message points the user at the fix (upgrade the client)
        # rather than drowning them in a pydantic error tree.
        for error in exc.errors():
            if error.get("loc") == ("schema_version",):
                raise RegistrySchemaMismatchError(
                    f"registry at {url} has an incompatible schema version. "
                    f"This client understands schema v{SCHEMA_VERSION}. "
                    f"Upgrade parsimony-mcp or pin --registry to an older URL."
                ) from exc
        raise RegistryMalformedError(
            f"registry at {url} failed schema validation: {exc.error_count()} error(s). "
            f"The registry may be corrupted or served by the wrong host."
        ) from exc


# --------------------------------------------------------------------- cache


def _read_cache(cache_path: Path) -> tuple[Registry, float] | None:
    """Return ``(Registry, mtime)`` if the cache exists and parses; else ``None``.

    A corrupted cache is ignored rather than raised — the cache is a
    performance optimization, not a source of truth. The caller falls
    through to the network path.
    """
    try:
        stat = cache_path.stat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        _LOG.warning("registry cache stat failed; ignoring cache", extra={"error": str(exc)})
        return None

    try:
        raw = cache_path.read_bytes()
    except OSError as exc:
        _LOG.warning("registry cache read failed; ignoring cache", extra={"error": str(exc)})
        return None

    try:
        registry = _parse_registry(raw, url=str(cache_path))
    except RegistryError as exc:
        _LOG.warning(
            "registry cache is unparseable; ignoring cache",
            extra={"cache_path": str(cache_path), "error": str(exc)},
        )
        return None

    return registry, stat.st_mtime


def _write_cache(cache_path: Path, raw: bytes) -> None:
    """Atomically write ``raw`` to ``cache_path`` at mode 0600 in a 0700 dir.

    Atomicity: temp file in the same directory, fsync, ``os.replace``.
    Tightened permissions: the cache may accrue metadata that ends up
    being useful to attackers (sign-up URLs, tag lists) — no reason
    to share-read.
    """
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # mkdir(mode=…) honors the umask; re-chmod to be explicit.
        # Windows doesn't enforce POSIX mode, hence the suppress.
        with contextlib.suppress(OSError):
            os.chmod(cache_path.parent, 0o700)
    except OSError as exc:
        _LOG.warning(
            "could not create registry cache dir; skipping write",
            extra={"path": str(cache_path.parent), "error": str(exc)},
        )
        return

    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=".registry-",
            suffix=".json.tmp",
            dir=str(cache_path.parent),
        )
        try:
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)
        with contextlib.suppress(OSError):
            os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, cache_path)
    except OSError as exc:
        _LOG.warning(
            "could not write registry cache; continuing without cache",
            extra={"path": str(cache_path), "error": str(exc)},
        )
        # Best-effort cleanup of any stray temp. ``tmp_name`` may be
        # unbound if mkstemp itself failed; suppress NameError too.
        with contextlib.suppress(OSError, NameError):
            os.unlink(tmp_name)
