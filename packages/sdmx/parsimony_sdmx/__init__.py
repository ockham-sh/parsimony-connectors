"""``parsimony-sdmx`` — SDMX connector plugin for the ``parsimony`` kernel.

Single-step plugin: every SDMX operation (dataset listing, per-dataset
series enumeration, live observation fetch) hits the agency endpoint
live through :mod:`parsimony_sdmx._isolation`'s subprocess boundary.
No on-disk parquet cache, no separate builder CLI — ``parsimony publish``
is the only command you need.

SDMX endpoints are public; no authentication required. Provider covers
ECB, Eurostat, IMF (IMF_DATA), and World Bank (WB_WDI) via the
``parsimony_sdmx.connectors._agencies`` registry.

Exports:

- :data:`CONNECTORS` — the plugin surface discovered via the
  ``parsimony.providers`` entry point group. Two enumerators
  (dataset-level + per-dataset series) and one live fetch connector.
- :data:`CATALOGS` — async generator consumed by
  :func:`parsimony.publish.publish`. Yields one static
  ``sdmx_datasets`` catalog plus one
  ``sdmx_series_<agency>_<dataset_id>`` catalog per live-discovered
  ``(agency, dataset_id)``. The dynamic namespace pattern is
  ``sdmx_series_<agency>_<dataset_id>`` — agency names lower-cased,
  dataset IDs are parsed back to canonical upper-case by
  :data:`RESOLVE_CATALOG`.
- :data:`RESOLVE_CATALOG` — on-demand lookup used by
  ``parsimony publish --only <namespace>``. Parses namespace strings
  back into ``(agency, dataset_id)`` without enumerating the full
  listing first (cheap: no SDMX calls).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable

from parsimony.result import Result

from parsimony_sdmx.connectors import CONNECTORS
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES, AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import (
    DATASETS_NAMESPACE,
    EnumerateDatasetsParams,
    enumerate_sdmx_datasets,
)
from parsimony_sdmx.connectors.enumerate_series import (
    EnumerateSeriesParams,
    enumerate_sdmx_series,
    series_namespace,
)

logger = logging.getLogger(__name__)


#: Param-less callable yielded inside a ``CATALOGS`` tuple.
_CatalogFn = Callable[[], Awaitable[Result]]


# ---------------------------------------------------------------------------
# Helper closures that bind the enumerator to specific params
# ---------------------------------------------------------------------------


def _datasets_fn() -> _CatalogFn:
    """Param-less callable for the ``sdmx_datasets`` catalog.

    Wrapping :func:`enumerate_sdmx_datasets` lets the publisher invoke
    it without knowing the empty-params shape.
    """

    async def _run() -> Result:
        return await enumerate_sdmx_datasets(EnumerateDatasetsParams())

    _run.__name__ = "enumerate_sdmx_datasets_bound"
    return _run


def _series_fn(agency: AgencyId, dataset_id: str) -> _CatalogFn:
    """Param-less callable for one ``sdmx_series_<agency>_<dataset_id>`` catalog."""

    async def _run() -> Result:
        return await enumerate_sdmx_series(EnumerateSeriesParams(agency=agency, dataset_id=dataset_id))

    _run.__name__ = f"enumerate_sdmx_series_{agency.value}_{dataset_id}"
    return _run


# ---------------------------------------------------------------------------
# CATALOGS — live dataset listing drives the fan-out
# ---------------------------------------------------------------------------


async def CATALOGS() -> AsyncIterator[tuple[str, _CatalogFn]]:
    """Yield every catalog this plugin can publish.

    Order:

    1. One static cross-agency ``sdmx_datasets`` catalog.
    2. One ``sdmx_series_<agency>_<dataset_id>`` catalog per
       ``(agency, dataset_id)`` returned by live dataset listing.

    The per-agency listing runs inside the dataset enumerator's own
    subprocess (no parallel listing — sdmx1 memory pressure accumulates
    in the parent if multiple listings run concurrently). Agencies that
    fail mid-listing are skipped with a warning; the run continues.
    """
    yield DATASETS_NAMESPACE, _datasets_fn()

    # One subprocess per agency for listing. Sequential — parallel sdmx1
    # memory pressure accumulates in the parent before each child exits.
    import asyncio

    from parsimony_sdmx._isolation import ListDatasetsError, list_datasets
    from parsimony_sdmx.connectors.enumerate_datasets import LISTING_TIMEOUT_S

    for agency in ALL_AGENCIES:
        try:
            records = await asyncio.to_thread(
                list_datasets, agency.value, LISTING_TIMEOUT_S
            )
        except ListDatasetsError as exc:
            logger.warning(
                "CATALOGS: listing failed for %s (%s): %s",
                agency.value,
                exc.kind,
                exc.message,
            )
            continue
        except Exception as exc:  # noqa: BLE001 — per-agency resilience
            logger.warning("CATALOGS: listing raised for %s: %s", agency.value, exc)
            continue

        # ESTAT lists "$DV_*" pseudo-dataflows (derived views: ~547 of 8208
        # for ESTAT). They aren't fetchable as series and the dataset_id
        # regex rejects them at validation time. Drop them here so we don't
        # spam pydantic ValidationError tracebacks for known-skip rows.
        skipped_dv = sum(1 for r in records if "$" in r.dataset_id)
        if skipped_dv:
            logger.info(
                "CATALOGS: %s: skipped %d derived-view ($DV_*) flow(s); %d publishable",
                agency.value,
                skipped_dv,
                len(records) - skipped_dv,
            )

        for record in records:
            if "$" in record.dataset_id:
                continue
            yield (
                series_namespace(agency, record.dataset_id),
                _series_fn(agency, record.dataset_id),
            )


# ---------------------------------------------------------------------------
# RESOLVE_CATALOG — cheap namespace → callable lookup (no SDMX calls)
# ---------------------------------------------------------------------------


def RESOLVE_CATALOG(namespace: str) -> _CatalogFn | None:
    """Return the catalog callable for ``namespace`` without listing datasets.

    Used by ``parsimony publish --only <namespace>`` so the caller
    doesn't wait for the full dataset listing (seconds per agency) when
    they already know the single namespace they want. No SDMX calls
    here — pure string parsing.

    ``sdmx_datasets`` routes to the static enumerator. Any namespace of
    the shape ``sdmx_series_<agency>_<dataset_id>`` is parsed back into
    ``(agency, dataset_id)`` and bound into a per-dataset callable.
    Longest-agency match wins (so ``imf_data_pgi`` resolves as
    ``IMF_DATA`` + ``pgi``, not ``IMF`` + ``data_pgi``).

    Returns ``None`` for namespaces this plugin doesn't own.
    """
    if namespace == DATASETS_NAMESPACE:
        return _datasets_fn()

    prefix = "sdmx_series_"
    if not namespace.startswith(prefix):
        return None

    rest = namespace[len(prefix):]
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
    # ESTAT $DV_* derived views are not fetchable; skip them at the
    # resolver too so --only on a $dv_ namespace short-circuits cleanly
    # rather than crashing the publisher with a pydantic ValidationError.
    if "$" in dataset_tail:
        return None

    # The catalog key is stored lowercase (``normalize_code`` in the
    # kernel), but every SDMX agency we wire up today uses uppercase
    # dataflow IDs upstream — so the canonical-case roundtrip is just
    # ``.upper()``. If a future agency surfaces a different convention,
    # push a per-provider ``canonical_dataset_id`` method onto the
    # ``CatalogProvider`` Protocol and call it here instead of this line.
    return _series_fn(agency, dataset_tail.upper())


__all__ = [
    "CATALOGS",
    "CONNECTORS",
    "RESOLVE_CATALOG",
]
