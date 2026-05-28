"""Agent-facing connectors: enumerators + fetch.

Exports:

- :data:`CONNECTORS` — collection passed to ``parsimony`` via the plugin
  entry point. Contains the two enumerators (dataset + series) and the
  live fetch connector.

SDMX endpoints are public; no auth required. ``HF_TOKEN`` is
read by :mod:`huggingface_hub` directly when :meth:`parsimony.Catalog.save`
uploads an ``hf://`` snapshot, not via parsimony's dep-binding system.

The three plugin surfaces:

- :func:`enumerate_sdmx_datasets` — produces catalog rows for per-agency
  namespaces ``sdmx_datasets_<agency>`` (one bundle per agency).
- :func:`enumerate_sdmx_series` — produces catalog rows for per-dataset
  namespaces ``sdmx_series_{agency}_{dataset_id}`` (one HF bundle per
  dataset, expected thousands total).
- :func:`sdmx_fetch` — live SDMX retrieval connector (one row per
  observation, schema in :func:`parsimony_sdmx.connectors.fetch._sdmx_fetch_output`).
- :func:`sdmx_datasets_search` / :func:`sdmx_series_search` — catalog-side
  search primitives.
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_sdmx.connectors.enumerate_datasets import enumerate_sdmx_datasets
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series
from parsimony_sdmx.connectors.fetch import sdmx_fetch
from parsimony_sdmx.connectors.search import (
    sdmx_datasets_search,
    sdmx_series_search,
)

CONNECTORS: Connectors = Connectors(
    [
        enumerate_sdmx_datasets,
        enumerate_sdmx_series,
        sdmx_fetch,
        sdmx_series_search,
        sdmx_datasets_search,
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
    "sdmx_datasets_search",
    "sdmx_fetch",
    "sdmx_series_search",
    "load",
]
