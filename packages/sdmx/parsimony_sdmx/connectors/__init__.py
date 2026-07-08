"""Agent-facing connectors: catalog search + fetch.

Exports:

- :data:`CONNECTORS` — collection passed to ``parsimony`` via the plugin
  entry point.

SDMX endpoints are public; no auth required. ``HF_TOKEN`` is
read by :mod:`huggingface_hub` directly when :meth:`parsimony.Catalog.save`
uploads an ``hf://`` snapshot.

The plugin surfaces:

- :func:`sdmx_datasets_search` — discover flows across agency dataset catalogs.
- :func:`sdmx_series_search` — columnar per-flow series search (catalog-backed).
- :func:`sdmx_dimension_search` — a flow dimension's values (catalog-backed).
- :func:`sdmx_fetch` — live SDMX retrieval.

Only published flows are searchable; an unpublished flow hard-errors (there is no
live fallback).
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_sdmx.connectors.datasets_search import sdmx_datasets_search
from parsimony_sdmx.connectors.dimension_search import sdmx_dimension_search
from parsimony_sdmx.connectors.fetch import sdmx_fetch
from parsimony_sdmx.connectors.series_search import sdmx_series_search

CONNECTORS: Connectors = Connectors(
    [
        sdmx_fetch,
        sdmx_datasets_search,
        sdmx_dimension_search,
        sdmx_series_search,
    ]
)


def load(*, catalog_root: str | None = None, catalog_lru_size: int | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with optional catalog runtime defaults bound."""
    from parsimony_sdmx.connectors.datasets_search import set_catalog_lru_size

    if catalog_lru_size is not None:
        set_catalog_lru_size(catalog_lru_size)
    if catalog_root is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_root=catalog_root)


__all__ = [
    "CONNECTORS",
    "sdmx_datasets_search",
    "sdmx_dimension_search",
    "sdmx_fetch",
    "sdmx_series_search",
    "load",
]
