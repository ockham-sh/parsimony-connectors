"""``enumerate_sdmx_datasets`` — live cross-agency dataset enumerator.

Walks every agency in :data:`~parsimony_sdmx.connectors._agencies.ALL_AGENCIES`
and calls each agency's live dataset-listing endpoint via
:func:`parsimony_sdmx._isolation.list_datasets`. One spawned subprocess
per agency — sdmx1 caches parsed structure messages at module scope
with no invalidation API, so every call that touches sdmx1 runs in a
fresh child (see :mod:`parsimony_sdmx._isolation` for the rationale).

Produces one row per ``(agency, dataset_id)`` with composite key
``"{agency}|{dataset_id}"`` — the kernel's ingest path treats that as
the primary key and agents can round-trip it into ``sdmx_fetch``.
"""

from __future__ import annotations

import asyncio
import logging

import pandas as pd
from parsimony.connector import enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel

from parsimony_sdmx._isolation import ListDatasetsError, list_datasets
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES
from parsimony_sdmx.core.models import DatasetRecord

logger = logging.getLogger(__name__)

#: Catalog namespace for the single cross-agency dataset bundle.
DATASETS_NAMESPACE = "sdmx_datasets"

#: Listing calls are typically faster than full dataset fetches; cap
#: them tighter so a hung agency-listing doesn't block the others.
LISTING_TIMEOUT_S: float = 600.0


class EnumerateDatasetsParams(BaseModel):
    """No parameters — the enumerator walks every agency."""


ENUMERATE_DATASETS_OUTPUT = OutputConfig(
    columns=[
        Column(
            name="code",
            role=ColumnRole.KEY,
            namespace=DATASETS_NAMESPACE,
            description="Composite dataset identifier: '{agency}|{dataset_id}'",
        ),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
    ]
)


@enumerator(
    output=ENUMERATE_DATASETS_OUTPUT,
    tags=["sdmx"],
)
async def enumerate_sdmx_datasets(
    params: EnumerateDatasetsParams,
    *,
    fetch_timeout_s: float = LISTING_TIMEOUT_S,
) -> pd.DataFrame:
    """List every SDMX dataset across every supported agency.

    Spawns one subprocess per agency (sequential, not parallel — parallel
    sdmx1 calls amplify memory pressure in the parent before each child
    exits). An agency that fails (timeout, crash, other exception) is
    skipped with a warning; the batch continues so a single flaky
    endpoint doesn't sink the whole listing.

    Raises :class:`~parsimony.errors.EmptyDataError` if *every* agency
    fails or returns empty — otherwise returns whatever the surviving
    agencies produced.
    """
    frames: list[pd.DataFrame] = []
    for agency in ALL_AGENCIES:
        try:
            records: list[DatasetRecord] = await asyncio.to_thread(
                list_datasets,
                agency.value,
                fetch_timeout_s,
            )
        except ListDatasetsError as exc:
            logger.warning(
                "dataset listing failed for agency %s (%s): %s",
                agency.value,
                exc.kind,
                exc.message,
            )
            continue
        except Exception as exc:  # noqa: BLE001 — per-agency resilience
            logger.warning(
                "dataset listing raised for agency %s: %s", agency.value, exc
            )
            continue

        if not records:
            continue

        frames.append(
            pd.DataFrame(
                {
                    "code": [f"{r.agency_id}|{r.dataset_id}" for r in records],
                    "title": [r.title for r in records],
                    "agency": [r.agency_id for r in records],
                    "dataset_id": [r.dataset_id for r in records],
                }
            )
        )

    if not frames:
        raise EmptyDataError(
            provider="sdmx",
            message="Live SDMX dataset listing produced no rows for any agency",
        )

    return pd.concat(frames, ignore_index=True)
