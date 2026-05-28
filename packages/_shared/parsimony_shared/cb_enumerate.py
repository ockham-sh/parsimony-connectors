"""Shared helpers for central-bank catalog enumerators."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

DESCRIPTION_CHAR_CAP = 1500

DEFAULT_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
DEFAULT_RETRY_BACKOFFS_S: tuple[float, ...] = (1.0, 2.0, 4.0)


def truncate_description(text: str, *, cap: int = DESCRIPTION_CHAR_CAP) -> str:
    """Cap a string at ``cap`` characters; return as-is if shorter."""
    if not text:
        return ""
    if len(text) <= cap:
        return text
    return text[:cap].rstrip()


def enumerate_descriptions(*parts: str, cap: int = DESCRIPTION_CHAR_CAP, sep: str = " ") -> str:
    """Join non-empty description fragments and cap total length for embedders."""
    joined = sep.join(p.strip() for p in parts if p and p.strip())
    return truncate_description(joined, cap=cap)


def parse_retry_after(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header; ``None`` if absent or malformed."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


@dataclass(frozen=True)
class MetadataCrawlConfig:
    """Throttling and retry policy for metadata enumeration crawls."""

    concurrency: int = 4
    inter_request_delay_s: float = 0.25
    retry_statuses: frozenset[int] = field(default_factory=lambda: DEFAULT_RETRY_STATUSES)
    retry_backoffs_s: tuple[float, ...] = DEFAULT_RETRY_BACKOFFS_S


class ThrottledJsonFetcher:
    """Async JSON GET helper with semaphore throttling, delay, and retries."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        provider: str,
        config: MetadataCrawlConfig | None = None,
        logger: logging.Logger | None = None,
        accept_non_json: Callable[[httpx.Response], bool] | None = None,
    ) -> None:
        self._client = client
        self._provider = provider
        self._config = config or MetadataCrawlConfig()
        self._logger = logger or logging.getLogger(__name__)
        self._semaphore = asyncio.Semaphore(self._config.concurrency)
        self._accept_non_json = accept_non_json

    @property
    def config(self) -> MetadataCrawlConfig:
        return self._config

    async def _get_with_retries(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        label: str | None = None,
    ) -> httpx.Response | None:
        """GET *url* with throttling and retries; ``None`` after exhausted attempts."""
        log_target = label or url
        async with self._semaphore:
            await asyncio.sleep(self._config.inter_request_delay_s)
            last_status: int | None = None
            last_error: str | None = None
            for attempt, backoff in enumerate((*self._config.retry_backoffs_s, None)):
                try:
                    response = await self._client.get(url, params=params)
                except httpx.HTTPError as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    if backoff is None:
                        break
                    await asyncio.sleep(backoff)
                    continue

                if response.status_code == 200:
                    if self._accept_non_json is not None and not self._accept_non_json(response):
                        return None
                    return response

                last_status = response.status_code
                if response.status_code in self._config.retry_statuses and backoff is not None:
                    wait = parse_retry_after(response) or backoff
                    self._logger.info(
                        "%s %s returned %s (attempt %d); retrying in %.1fs",
                        self._provider,
                        log_target,
                        response.status_code,
                        attempt + 1,
                        wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                break

            self._logger.warning(
                "%s fetch failed for %s after retries (last_status=%s, last_error=%s)",
                self._provider,
                log_target,
                last_status,
                last_error,
            )
            return None

    async def get_json(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        label: str | None = None,
    ) -> Any | None:
        """GET *url* and return parsed JSON, or ``None`` after exhausted retries."""
        log_target = label or url
        response = await self._get_with_retries(url, params=params, label=label)
        if response is None:
            return None
        try:
            return response.json()
        except ValueError as exc:
            self._logger.warning("%s %s returned non-JSON body: %s", self._provider, log_target, exc)
            return None

    async def get_text(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        label: str | None = None,
    ) -> str | None:
        """GET *url* and return response text, or ``None`` after exhausted retries."""
        response = await self._get_with_retries(url, params=params, label=label)
        if response is None:
            return None
        return response.text

    async def get_content(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        label: str | None = None,
    ) -> bytes | None:
        """GET *url* and return raw response bytes, or ``None`` after exhausted retries."""
        response = await self._get_with_retries(url, params=params, label=label)
        if response is None:
            return None
        return response.content


__all__ = [
    "DESCRIPTION_CHAR_CAP",
    "DEFAULT_RETRY_BACKOFFS_S",
    "DEFAULT_RETRY_STATUSES",
    "MetadataCrawlConfig",
    "ThrottledJsonFetcher",
    "enumerate_descriptions",
    "parse_retry_after",
    "truncate_description",
]
