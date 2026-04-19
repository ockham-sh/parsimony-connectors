"""Plan generators for ``parsimony.bundles`` discovery.

The new bundle pipeline (``parsimony.bundles``) drives every plugin's
publish flow through ``CatalogDynamicSpec.plan`` — an async generator
that yields one :class:`~parsimony.bundles.CatalogPlan` per bundle the
plugin wants built. SDMX has thousands of per-dataset series bundles, so
the plan generator walks the on-disk flat-catalog parquet files and
emits one plan item per ``(agency, dataset_id)`` pair.

The on-disk root is :data:`DEFAULT_OUTPUTS_ROOT` (sibling to the package),
overridable via the ``PARSIMONY_SDMX_OUTPUTS_ROOT`` env var. Missing
agencies are silently skipped — callers running ``parsimony bundles
plan`` against a workspace where only one agency has been built locally
should see only that agency's bundles.

This module is import-cheap: it imports ``pyarrow`` lazily inside
:func:`plan_sdmx_series` so importing the plugin's surface (which the
``parsimony list-plugins`` discovery does eagerly) doesn't pay arrow's
cost when no one is publishing yet.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

from parsimony.bundles import CatalogPlan

from parsimony_sdmx.connectors._agencies import (
    ALL_AGENCIES,
    AgencyId,
    to_namespace_token,
)


def _outputs_root() -> Path:
    """Resolve the flat-catalog outputs root from env var or default.

    The default is imported lazily — :mod:`parsimony_sdmx.connectors.enumerate_datasets`
    pulls in pyarrow+pandas at module load, which we don't want when the
    plan generator is only being inspected (e.g. by the discovery walk).
    """
    env = os.environ.get("PARSIMONY_SDMX_OUTPUTS_ROOT")
    if env:
        return Path(env)
    from parsimony_sdmx.connectors.enumerate_datasets import DEFAULT_OUTPUTS_ROOT

    return DEFAULT_OUTPUTS_ROOT


def _series_namespace(agency: AgencyId, dataset_id: str) -> str:
    """Compose the per-dataset series namespace from agency + dataset id.

    The template literal is inlined here (rather than imported from
    :mod:`parsimony_sdmx.connectors.enumerate_series`) to avoid a
    circular import — ``enumerate_series`` declares ``catalog=`` with
    a callable that lives in this module.
    """
    return f"sdmx_series_{to_namespace_token(agency)}_{dataset_id.lower()}"


async def plan_sdmx_series() -> AsyncIterator[CatalogPlan]:
    """Yield one plan per ``(agency, dataset_id)`` pair found on disk.

    Reads each agency's ``outputs/{AGENCY}/datasets.parquet`` and emits a
    :class:`CatalogPlan` for every row. Empty / absent agency files are
    skipped silently — local workspaces don't always have every agency.

    Plan params shape::

        {"agency": "ECB", "dataset_id": "YC"}

    These map 1:1 to :class:`~parsimony_sdmx.connectors.enumerate_series.EnumerateSeriesParams`
    so the ``parsimony bundles`` runner adapter constructs the model
    directly via ``EnumerateSeriesParams(**plan.params)``.
    """
    import pyarrow.parquet as pq

    root = _outputs_root()
    for agency in ALL_AGENCIES:
        path = root / agency.value / "datasets.parquet"
        if not path.exists():
            continue
        table = pq.read_table(path, columns=["dataset_id"])
        for dataset_id in table.column("dataset_id").to_pylist():
            yield CatalogPlan(
                namespace=_series_namespace(agency, dataset_id),
                params={"agency": agency.value, "dataset_id": dataset_id},
            )


__all__ = [
    "plan_sdmx_series",
]
