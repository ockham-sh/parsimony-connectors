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
- ``sdmx_fetch`` — live SDMX retrieval (Task 6, not yet wired here).
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_sdmx.connectors.enumerate_datasets import enumerate_sdmx_datasets
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series
from parsimony_sdmx.connectors.fetch import sdmx_fetch

CONNECTORS: Connectors = Connectors([enumerate_sdmx_datasets, enumerate_sdmx_series, sdmx_fetch])


__all__ = [
    "CONNECTORS",
    "enumerate_sdmx_datasets",
    "enumerate_sdmx_series",
    "sdmx_fetch",
]
