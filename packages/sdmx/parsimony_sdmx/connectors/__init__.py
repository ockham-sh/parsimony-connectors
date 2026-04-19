"""Agent-facing connectors: enumerators + fetch.

Exports:

- :data:`CONNECTORS` — collection passed to ``parsimony`` via the plugin
  entry point. Contains the two enumerators (dataset + series) and the
  live fetch connector.
- :data:`ENV_VARS` — empty. SDMX endpoints are public; ``HF_TOKEN`` is
  read directly by :class:`parsimony.stores.hf_bundle.store.HFBundleCatalogStore`
  when present, not via parsimony's dep-binding system.

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

ENV_VARS: dict[str, str] = {}


__all__ = [
    "CONNECTORS",
    "ENV_VARS",
    "enumerate_sdmx_datasets",
    "enumerate_sdmx_series",
    "sdmx_fetch",
]
