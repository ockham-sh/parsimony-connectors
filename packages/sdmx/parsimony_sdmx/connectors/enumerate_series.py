"""``enumerate_sdmx_series`` — live per-dataset series enumerator.

Hits the live SDMX agency endpoint for one ``(agency, dataset_id)`` via
:func:`parsimony_sdmx._isolation.fetch_series`. sdmx1's module-scope
cache is unbounded and can only be flushed by process death, so every
call spawns a fresh child (see :mod:`parsimony_sdmx._isolation` for the
full rationale).

The per-dataset catalog namespace
(``sdmx_series_<agency_lower>_<dataset_id_lower>``) is supplied by the
catalog at ingest time via ``Catalog.add_from_result`` — this
enumerator's KEY column carries no namespace declaration.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import pandas as pd
from parsimony.connector import enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field, field_validator

from parsimony_sdmx._isolation import (
    FETCH_SERIES_DEFAULT_TIMEOUT_S,
    fetch_series,
)
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import SeriesRecord


def series_namespace(agency: AgencyId | str, dataset_id: str) -> str:
    """Compose the canonical per-dataset series namespace string.

    ``AgencyId.ECB`` + ``"YC"`` → ``"sdmx_series_ecb_yc"``. Used by
    ``parsimony_sdmx.CATALOGS`` when yielding catalog entries and by
    ``parsimony_sdmx.RESOLVE_CATALOG`` when round-tripping a namespace
    string back to an ``(agency, dataset_id)`` pair.
    """
    raw = agency.value if isinstance(agency, AgencyId) else str(agency)
    return f"sdmx_series_{raw.lower()}_{dataset_id.lower()}"


class EnumerateSeriesParams(BaseModel):
    """Parameters for per-dataset series enumeration.

    Accepts both uppercase (``"ECB"``) and lowercase (``"ecb"``) agency
    inputs so round-tripping through :func:`parsimony_sdmx.RESOLVE_CATALOG`
    (which parses lowercase tokens out of namespace strings) flows
    through without extra casing gymnastics on the caller side.
    """

    agency: Annotated[AgencyId, Field(description="SDMX source ID (ECB, ESTAT, IMF_DATA, WB_WDI)")]
    dataset_id: Annotated[
        str,
        Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._\-]*$"),
    ]

    @field_validator("agency", mode="before")
    @classmethod
    def _upcase_agency(cls, v: str | AgencyId) -> str | AgencyId:
        if isinstance(v, str):
            return v.upper()
        return v


ENUMERATE_SERIES_OUTPUT = OutputConfig(
    columns=[
        # KEY namespace is deliberately unset — the catalog's own ``name``
        # becomes the default namespace at ingest time, letting one
        # enumerator feed many per-dataset catalogs without templating.
        Column(
            name="code",
            role=ColumnRole.KEY,
            description="SDMX series key (dot-separated dimension values).",
        ),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="agency", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
    ]
)


@enumerator(
    output=ENUMERATE_SERIES_OUTPUT,
    tags=["sdmx"],
)
async def enumerate_sdmx_series(
    params: EnumerateSeriesParams,
    *,
    fetch_timeout_s: float = FETCH_SERIES_DEFAULT_TIMEOUT_S,
) -> pd.DataFrame:
    """List every series inside one SDMX dataset, hitting the live agency endpoint.

    Projects one row per series into the declared OutputConfig schema
    (KEY=code, TITLE=title, METADATA=(agency, dataset_id)). The
    ``@enumerator`` decorator wraps the returned DataFrame into a
    :class:`Result` with :data:`ENUMERATE_SERIES_OUTPUT` attached so
    ``catalog.add_from_result()`` can read the schema. The catalog
    name (set at publish time by :data:`parsimony_sdmx.CATALOGS`)
    becomes the namespace at ingest time.

    ``fetch_timeout_s`` bounds the subprocess wall-clock; a timeout or
    other subprocess failure raises :class:`~parsimony_sdmx._isolation.FetchSeriesError`
    which the kernel's publish loop catches per-namespace and reports
    as a failed bundle.
    """
    records: list[SeriesRecord] = await asyncio.to_thread(
        fetch_series,
        params.agency.value,
        params.dataset_id,
        fetch_timeout_s,
    )

    if not records:
        raise EmptyDataError(
            provider="sdmx",
            message=f"Live SDMX returned zero series for {params.agency.value}/{params.dataset_id}",
        )

    return pd.DataFrame(
        {
            "code": [r.id for r in records],
            "title": [r.title for r in records],
            "agency": params.agency.value,
            "dataset_id": params.dataset_id,
        }
    )
