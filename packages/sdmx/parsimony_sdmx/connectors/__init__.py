"""Agent-facing connectors: enumerators + fetch.

Exports:

- :data:`CONNECTORS` — collection passed to ``parsimony`` via the plugin
  entry point. Contains the two enumerators (dataset + series) and the
  live fetch connector.

SDMX endpoints are public; no auth required. ``HF_TOKEN`` is
read by :mod:`huggingface_hub` directly when :meth:`parsimony.Catalog.push`
uploads an ``hf://`` snapshot, not via parsimony's dep-binding system.

The three plugin surfaces:

- :func:`enumerate_sdmx_datasets` — produces catalog rows for namespace
  ``sdmx_datasets`` (one bundle, all agencies).
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


__all__ = [
    "CONNECTORS",
    "enumerate_sdmx_datasets",
    "enumerate_sdmx_series",
    "sdmx_datasets_search",
    "sdmx_fetch",
    "sdmx_series_search",
]
