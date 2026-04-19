"""Shared HTTP session factory with one-layer retry, split timeouts, HTTPS-only.

One ``requests.Session`` per subprocess. The mounted ``HTTPAdapter`` is
the **sole** owner of HTTP-level retries — ``sdmx1`` internal retries
must be disabled and application code must not wrap calls in local
retry loops.

This module also hosts :func:`classify_exception` because exception →
``FailureKind`` classification transitively depends on ``requests``'s
exception hierarchy, and ``core/`` must stay I/O-free.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from parsimony_sdmx.core.errors import (
    CodelistMissingError,
    ParquetWriteError,
    SdmxFetchError,
    TitleBuildError,
)
from parsimony_sdmx.core.outcomes import FailureKind

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "parsimony-sdmx/0.1 (+https://github.com/parsimony-sdmx)"
DEFAULT_MAX_RESPONSE_BYTES = 500 * 1024 * 1024  # 500 MB
DEFAULT_STREAM_CHUNK = 64 * 1024


@dataclass(frozen=True, slots=True)
class HttpConfig:
    """Tuning knobs for outbound HTTP. All timeouts are in seconds."""

    connect_timeout: float = 10.0
    read_timeout: float = 120.0
    max_retries: int = 3
    backoff_factor: float = 1.0
    pool_connections: int = 16
    pool_maxsize: int = 16
    user_agent: str = DEFAULT_USER_AGENT
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES

    @property
    def timeout(self) -> tuple[float, float]:
        return (self.connect_timeout, self.read_timeout)


RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)
RETRY_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def build_session(config: HttpConfig | None = None) -> requests.Session:
    """Construct a configured ``requests.Session`` for this subprocess.

    * ``HTTPAdapter`` with urllib3 ``Retry`` (backoff, jitter-like via
      ``backoff_factor``, ``Retry-After`` honouring, status forcelist
      ``{429, 500, 502, 503, 504}``).
    * Connection pool sized to the expected thread fan-out.
    * ``User-Agent`` set for upstream identification.
    * Only ``https://`` is mounted — ``http://`` calls via
      :func:`bounded_get` are rejected at the wrapper.
    """
    cfg = config or HttpConfig()
    retry = Retry(
        total=cfg.max_retries,
        connect=cfg.max_retries,
        read=cfg.max_retries,
        status=cfg.max_retries,
        backoff_factor=cfg.backoff_factor,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=RETRY_METHODS,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=cfg.pool_connections,
        pool_maxsize=cfg.pool_maxsize,
        max_retries=retry,
    )
    session = requests.Session()
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": cfg.user_agent})
    return session


def bounded_get(
    session: requests.Session,
    url: str,
    config: HttpConfig | None = None,
    extra_headers: dict[str, str] | None = None,
) -> bytes:
    """Fetch ``url`` with HTTPS enforcement, split timeouts, and a byte cap.

    Streams the response in chunks and aborts if the cumulative bytes
    exceed ``config.max_response_bytes``. Raises :class:`SdmxFetchError`
    for any HTTP or stream failure so the caller can classify via
    :func:`parsimony_sdmx.core.outcomes.classify_exception`.
    """
    cfg = config or HttpConfig()
    _require_https(url)
    try:
        response = session.get(
            url,
            timeout=cfg.timeout,
            stream=True,
            headers=extra_headers or None,
        )
    except requests.RequestException as exc:
        raise SdmxFetchError(f"GET {url} failed: {exc}") from exc

    try:
        response.raise_for_status()
        buf = bytearray()
        for chunk in response.iter_content(chunk_size=DEFAULT_STREAM_CHUNK):
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) > cfg.max_response_bytes:
                raise SdmxFetchError(
                    f"Response from {url} exceeded {cfg.max_response_bytes} bytes"
                )
        return bytes(buf)
    finally:
        response.close()


def _require_https(url: str) -> None:
    if not url.startswith("https://"):
        raise SdmxFetchError(f"Non-HTTPS URL rejected: {url}")


@contextmanager
def bounded_stream(
    session: requests.Session,
    url: str,
    config: HttpConfig | None = None,
    extra_headers: dict[str, str] | None = None,
) -> Iterator[requests.Response]:
    """Context manager yielding a streamed response with HTTPS enforcement.

    Unlike :func:`bounded_get`, this does not buffer the body — callers
    consume ``response.raw`` (or ``response.iter_content``) directly and
    are responsible for enforcing their own byte cap as they read. The
    caller should use ``response.raw`` with an ``iterparse`` or similar
    streaming consumer to keep peak memory below ``max_response_bytes``.

    The response is always closed on context exit.
    """
    cfg = config or HttpConfig()
    _require_https(url)
    try:
        response = session.get(
            url,
            timeout=cfg.timeout,
            stream=True,
            headers=extra_headers or None,
        )
    except requests.RequestException as exc:
        raise SdmxFetchError(f"GET {url} failed: {exc}") from exc
    try:
        response.raise_for_status()
        # Decode gzip/deflate transparently — raw must reflect decoded bytes.
        response.raw.decode_content = True
        yield response
    finally:
        response.close()


def classify_exception(exc: BaseException) -> FailureKind:
    """Map a caught exception to a :class:`FailureKind`.

    The taxonomy is operator-facing — just enough to hint at what to do
    next when scanning a failure summary. Nothing in the pipeline
    branches on it, so we keep it coarse: HTTP/network, timeout, parse,
    OOM, or unknown. Bare ``ValueError`` / ``KeyError`` / ``TypeError``
    fall through to ``UNKNOWN`` so programmer bugs surface as bugs.
    """
    if isinstance(exc, requests.exceptions.Timeout):
        return FailureKind.TIMEOUT
    if isinstance(exc, (requests.exceptions.RequestException, SdmxFetchError)):
        return FailureKind.HTTP_ERROR
    if isinstance(exc, (TitleBuildError, ParquetWriteError)):
        return FailureKind.PARSE_ERROR
    if isinstance(exc, (etree.XMLSyntaxError, json.JSONDecodeError)):
        return FailureKind.PARSE_ERROR
    _ = CodelistMissingError  # subclass of TitleBuildError, covered above.
    return FailureKind.UNKNOWN
