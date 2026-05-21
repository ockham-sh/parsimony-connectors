"""Catalog loading and BM25 fallback for Bank of Canada search."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from huggingface_hub.errors import RepositoryNotFoundError
from parsimony.catalog import BM25Index, Catalog, entries_from_result
from parsimony.errors import ConnectorError
from parsimony.result import Result

logger = logging.getLogger(__name__)

PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV = "PARSIMONY_BOC_CATALOG_FALLBACK_BM25"
DEFAULT_CATALOG_URL = "hf://parsimony-dev/boc"


@dataclass
class BocRuntimeConfig:
    """Provider-local runtime defaults set by :func:`load`."""

    catalog_url: str | None = None
    fallback_bm25: bool = False


_runtime = BocRuntimeConfig()
_catalog: Catalog | None = None
_catalog_key: tuple[str, bool] | None = None
_catalog_lock = asyncio.Lock()


def configure(*, catalog_url: str | None = None, fallback_bm25: bool = False) -> None:
    """Set provider-local defaults for catalog loading."""
    _runtime.catalog_url = catalog_url
    _runtime.fallback_bm25 = fallback_bm25


def _fallback_enabled(explicit: bool | None) -> bool:
    if explicit is not None:
        return explicit
    if _runtime.fallback_bm25:
        return True
    raw = os.environ.get(PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV, "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def bm25_catalog(name: str) -> Catalog:
    """Return a cheap BM25-only catalog layout for runtime fallback builds."""
    return Catalog(
        name,
        indexes=[
            BM25Index("code_bm25", field="code"),
            BM25Index("title_bm25", field="title"),
            BM25Index("description_bm25", field="description"),
        ],
        default_field="title",
    )


async def build_bm25_catalog_from_enumeration(
    *,
    name: str,
    enumerate: Callable[[], Awaitable[Result]],
) -> Catalog:
    """Enumerate live provider entities and build an in-process BM25 catalog."""
    result = await enumerate()
    catalog = bm25_catalog(name)
    catalog.set_entries(entries_from_result(result))
    await catalog.build()
    logger.info("Built BM25 fallback catalog %s with %d entries", catalog.name, len(catalog))
    return catalog


async def get_catalog(
    *,
    catalog_url: str | None = None,
    fallback_bm25: bool | None = None,
    enumerate: Callable[[], Awaitable[Result]] | None = None,
    catalog_name: str = "boc",
) -> Catalog:
    """Load a published catalog snapshot, optionally falling back to BM25 enumeration."""
    global _catalog, _catalog_key

    url = catalog_url or _runtime.catalog_url or os.environ.get(
        "PARSIMONY_BOC_CATALOG_URL", DEFAULT_CATALOG_URL
    )
    use_fallback = _fallback_enabled(fallback_bm25)
    cache_key = (url, use_fallback)

    async with _catalog_lock:
        if _catalog is not None and _catalog_key == cache_key:
            return _catalog

        try:
            loaded = await Catalog.load(url)
        except (FileNotFoundError, RepositoryNotFoundError) as exc:
            if not use_fallback:
                raise ConnectorError(
                    (
                        f"BoC catalog not available at {url!r}. "
                        "Publish a snapshot or enable BM25 fallback "
                        f"({PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV}=1 or fallback_bm25=True). "
                        "DO NOT retry unless fallback is enabled."
                    ),
                    provider="boc",
                ) from exc
            if enumerate is None:
                raise ConnectorError(
                    "BM25 fallback requested but no enumerator was configured.",
                    provider="boc",
                ) from exc
            logger.warning(
                "Published BoC catalog unavailable at %s; building BM25 fallback (%s)",
                url,
                exc,
            )
            loaded = await build_bm25_catalog_from_enumeration(name=catalog_name, enumerate=enumerate)
        except ValueError as exc:
            if "integrity check failed" in str(exc):
                raise ConnectorError(
                    f"BoC catalog at {url!r} failed integrity validation. DO NOT retry.",
                    provider="boc",
                ) from exc
            if not use_fallback:
                raise ConnectorError(
                    (
                        f"BoC catalog not available at {url!r}. "
                        "Publish a snapshot or enable BM25 fallback "
                        f"({PARSIMONY_BOC_CATALOG_FALLBACK_BM25_ENV}=1 or fallback_bm25=True). "
                        "DO NOT retry unless fallback is enabled."
                    ),
                    provider="boc",
                ) from exc
            if enumerate is None:
                raise ConnectorError(
                    "BM25 fallback requested but no enumerator was configured.",
                    provider="boc",
                ) from exc
            logger.warning(
                "Published BoC catalog unavailable at %s; building BM25 fallback (%s)",
                url,
                exc,
            )
            loaded = await build_bm25_catalog_from_enumeration(name=catalog_name, enumerate=enumerate)

        _catalog = loaded
        _catalog_key = cache_key
        return loaded


def _clear_catalog_cache_for_tests() -> None:
    """Drop cached catalogs. Test-only."""
    global _catalog, _catalog_key
    _catalog = None
    _catalog_key = None
