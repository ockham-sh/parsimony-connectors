"""Agent-facing connectors: enumerators + fetch + catalog search.

Exports:

- :data:`CONNECTORS` — collection passed to ``parsimony`` via the plugin
  entry point.

SDMX endpoints are public; no auth required. ``HF_TOKEN`` is
read by :mod:`huggingface_hub` directly when :meth:`parsimony.Catalog.save`
uploads an ``hf://`` snapshot.

The plugin surfaces:

- :func:`enumerate_sdmx_datasets` — per-agency ``sdmx_datasets_<agency>`` catalogs.
- :func:`enumerate_sdmx_series` — scoped keys-only series discovery (live).
- :func:`sdmx_fetch` — live SDMX retrieval.
- :func:`sdmx_datasets_search` / :func:`sdmx_codelist_search` — catalog search.
- :func:`sdmx_series_search` — columnar per-flow series search (catalog-backed).
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_sdmx.connectors.enumerate_datasets import enumerate_sdmx_datasets
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series
from parsimony_sdmx.connectors.fetch import sdmx_fetch
from parsimony_sdmx.connectors.search import (
    sdmx_codelist_search,
    sdmx_datasets_search,
)
from parsimony_sdmx.connectors.series_search import sdmx_series_search

CONNECTORS: Connectors = Connectors(
    [
        enumerate_sdmx_datasets,
        enumerate_sdmx_series,
        sdmx_fetch,
        sdmx_codelist_search,
        sdmx_datasets_search,
        sdmx_series_search,
    ]
)


def load(*, catalog_root: str | None = None, catalog_lru_size: int | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with optional catalog runtime defaults bound."""
    from parsimony_sdmx.connectors.search import set_catalog_lru_size

    if catalog_lru_size is not None:
        set_catalog_lru_size(catalog_lru_size)
    if catalog_root is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_root=catalog_root)


__all__ = [
    "CONNECTORS",
    "enumerate_sdmx_datasets",
    "enumerate_sdmx_series",
    "sdmx_codelist_search",
    "sdmx_datasets_search",
    "sdmx_fetch",
    "sdmx_series_search",
    "load",
]
