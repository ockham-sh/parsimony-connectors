"""``parsimony-sdmx`` — SDMX connector plugin for the ``parsimony`` kernel.

Exports:

- :data:`CONNECTORS` — the plugin surface discovered via the
  ``parsimony.providers`` entry point group. Two enumerators (dataset-level
  + per-dataset series) and one live fetch connector.
- :data:`ENV_VARS` — empty. SDMX endpoints are public.
- :data:`CATALOGS` — async generator consumed by
  :func:`parsimony.publish.publish`. Yields one static ``sdmx_datasets``
  catalog plus one ``sdmx_series_<agency>_<dataset>`` catalog per
  ``(agency, dataset_id)`` found on disk (see
  :func:`plan_series_catalogs` for the on-disk walk).
- :data:`RESOLVE_CATALOG` — on-demand lookup used by
  ``parsimony publish --only <namespace>``. Returns the param-less
  catalog callable for one specific namespace without walking the full
  :data:`CATALOGS` generator.
- :data:`PROVIDER_METADATA` — static provider-level facts.

Discovery is driven by the kernel via entry points declared in
``pyproject.toml``. No manual registration required — users
``pip install parsimony-sdmx`` and the plugin appears in
``parsimony list``.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

from parsimony.result import Result

from parsimony_sdmx.connectors import CONNECTORS, ENV_VARS
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES, AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import (
    DATASETS_NAMESPACE,
    DEFAULT_OUTPUTS_ROOT,
    enumerate_sdmx_datasets,
)
from parsimony_sdmx.connectors.enumerate_series import (
    EnumerateSeriesParams,
    enumerate_sdmx_series,
    series_namespace,
)

__version__ = "0.3.0"


PROVIDER_METADATA: dict[str, Any] = {
    "agencies": [a.value for a in ALL_AGENCIES],
    "namespaces": {
        "static": [DATASETS_NAMESPACE],
        "dynamic_pattern": "sdmx_series_<agency>_<dataset_id>",
    },
    "plugin_version": __version__,
}


#: Callable yielded inside a ``CATALOGS`` tuple.
_CatalogFn = Callable[[], Awaitable[Result]]


# ---------------------------------------------------------------------------
# On-disk walk (used by CATALOGS and RESOLVE_CATALOG)
# ---------------------------------------------------------------------------


def _outputs_root() -> Path:
    """Resolve the flat-catalog outputs root from env or the library default.

    Reading this lazily at call time (not at import) keeps plugin discovery
    cheap: ``parsimony list`` doesn't touch disk for SDMX.
    """
    env = os.environ.get("PARSIMONY_SDMX_OUTPUTS_ROOT")
    if env:
        return Path(env)
    return DEFAULT_OUTPUTS_ROOT


def _read_agency_datasets(root: Path, agency: AgencyId) -> list[str]:
    """Return every ``dataset_id`` under ``root/{AGENCY}/datasets.parquet``.

    Returns an empty list when the agency's parquet is absent — local
    workspaces don't always build every agency. pyarrow is imported lazily
    so the bare plugin import stays cheap.
    """
    path = root / agency.value / "datasets.parquet"
    if not path.exists():
        return []
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["dataset_id"])
    return [str(d) for d in table.column("dataset_id").to_pylist()]


def _series_fn(agency: AgencyId, dataset_id: str) -> _CatalogFn:
    """Build a param-less catalog callable bound to ``(agency, dataset_id)``.

    The kernel's publisher calls the tuple's second element with no args
    (see :func:`parsimony.publish._invoke`). Because ``enumerate_sdmx_series``
    is a :class:`~parsimony.connector.Connector` that needs a params model
    and an ``outputs_root`` dep, we resolve both here and expose a plain
    async closure — the kernel's dispatch detects a non-Connector callable
    and invokes it directly.

    ``outputs_root`` is re-read per call (rather than captured at closure
    creation) so the same closure produced by :data:`CATALOGS` or
    :func:`RESOLVE_CATALOG` honours a late ``PARSIMONY_SDMX_OUTPUTS_ROOT``
    change.
    """

    async def _run() -> Result:
        bound = enumerate_sdmx_series.bind_deps(outputs_root=_outputs_root())
        return await bound(EnumerateSeriesParams(agency=agency, dataset_id=dataset_id))

    _run.__name__ = f"enumerate_sdmx_series_{agency.value}_{dataset_id}"
    return _run


def _datasets_fn() -> _CatalogFn:
    """Return a param-less callable for the ``sdmx_datasets`` catalog.

    Wrapping :func:`enumerate_sdmx_datasets` lets us bind ``outputs_root``
    to whatever :func:`_outputs_root` resolves at call time — the raw
    Connector would always use its compiled-in default.
    """

    async def _run() -> Result:
        bound = enumerate_sdmx_datasets.bind_deps(outputs_root=_outputs_root())
        from parsimony_sdmx.connectors.enumerate_datasets import EnumerateDatasetsParams

        return await bound(EnumerateDatasetsParams())

    _run.__name__ = "enumerate_sdmx_datasets_bound"
    return _run


# ---------------------------------------------------------------------------
# CATALOGS — async generator consumed by ``parsimony publish``
# ---------------------------------------------------------------------------


async def CATALOGS() -> AsyncIterator[tuple[str, _CatalogFn]]:
    """Yield every catalog this plugin can publish.

    Order:

    1. One static cross-agency ``sdmx_datasets`` catalog.
    2. One ``sdmx_series_<agency>_<dataset_id>`` catalog per
       ``(agency, dataset_id)`` with a local parquet under the flat-catalog
       outputs root (``PARSIMONY_SDMX_OUTPUTS_ROOT`` or the packaged default).

    Missing agencies are skipped silently — local workspaces may only have
    a subset built.
    """
    yield DATASETS_NAMESPACE, _datasets_fn()

    root = _outputs_root()
    for agency in ALL_AGENCIES:
        dataset_ids = _read_agency_datasets(root, agency)
        for dataset_id in dataset_ids:
            yield series_namespace(agency, dataset_id), _series_fn(agency, dataset_id)


# ---------------------------------------------------------------------------
# RESOLVE_CATALOG — on-demand lookup for ``parsimony publish --only <ns>``
# ---------------------------------------------------------------------------


def RESOLVE_CATALOG(namespace: str) -> _CatalogFn | None:
    """Return the catalog callable for ``namespace`` without walking ``CATALOGS``.

    Used by ``parsimony publish --only <namespace>`` to avoid the O(N)
    disk walk when the caller already knows the single namespace they want.
    Returns ``None`` for namespaces this plugin doesn't own.

    ``sdmx_datasets`` routes to the static enumerator. Any namespace of the
    shape ``sdmx_series_<agency>_<dataset_id>`` is parsed back into
    ``(agency, dataset_id)`` and bound into a per-dataset series callable.
    The agency token must match one of :data:`ALL_AGENCIES` (case-insensitive);
    unknown agencies return ``None``.
    """
    if namespace == DATASETS_NAMESPACE:
        return _datasets_fn()

    prefix = "sdmx_series_"
    if not namespace.startswith(prefix):
        return None

    rest = namespace[len(prefix):]
    # Match the longest agency token that fits (so ``imf_data_pgi`` parses
    # as agency=IMF_DATA / dataset_id=PGI rather than agency=IMF).
    agency: AgencyId | None = None
    dataset_tail: str | None = None
    for candidate in sorted(ALL_AGENCIES, key=lambda a: len(a.value), reverse=True):
        token = candidate.value.lower()
        if rest == token or rest.startswith(f"{token}_"):
            agency = candidate
            dataset_tail = rest[len(token) + 1:] if rest.startswith(f"{token}_") else ""
            break
    if agency is None or not dataset_tail:
        return None

    return _series_fn(agency, dataset_tail)


__all__ = [
    "CATALOGS",
    "CONNECTORS",
    "ENV_VARS",
    "PROVIDER_METADATA",
    "RESOLVE_CATALOG",
    "__version__",
]
