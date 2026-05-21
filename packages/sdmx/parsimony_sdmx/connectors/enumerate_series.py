"""``enumerate_sdmx_series`` — live per-dataset series enumerator.

Hits the live SDMX agency endpoint for one ``(agency, dataset_id)`` via
:func:`parsimony_sdmx._isolation.fetch_series`. sdmx1's module-scope
cache is unbounded and can only be flushed by process death, so every
call spawns a fresh child (see :mod:`parsimony_sdmx._isolation` for the
full rationale).

The provider build script wraps this enumerator for catalog building and stamps
the per-dataset catalog namespace
(``sdmx_series_<agency_lower>_<dataset_id_lower>``) onto the returned
``Result`` schema before converting it with ``entries_from_result``.
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
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord


def series_namespace(agency: AgencyId | str, dataset_id: str) -> str:
    """Compose the canonical per-dataset series namespace string.

    ``AgencyId.ECB`` + ``"YC"`` → ``"sdmx_series_ecb_yc"``. Used by
    the provider build script when round-tripping a namespace string back
    to an ``(agency, dataset_id)`` pair.
    """
    raw = agency.value if isinstance(agency, AgencyId) else str(agency)
    return f"sdmx_series_{raw.lower()}_{dataset_id.lower()}"


class EnumerateSeriesParams(BaseModel):
    """Parameters for per-dataset series enumeration.

    Accepts both uppercase (``"ECB"``) and lowercase (``"ecb"``) agency
    inputs so build-script namespace parsing flows through without extra
    casing gymnastics on the caller side.
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
        Column(
            name="code",
            role=ColumnRole.KEY,
            description="SDMX series key (dot-separated dimension values).",
        ),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="*", role=ColumnRole.METADATA),
    ]
)


@enumerator(
    output=ENUMERATE_SERIES_OUTPUT,
    tags=["sdmx"],
)
async def enumerate_sdmx_series(
    agency: AgencyId,
    dataset_id: str,
    fetch_timeout_s: float = FETCH_SERIES_DEFAULT_TIMEOUT_S,
) -> pd.DataFrame:
    """List every series inside one SDMX dataset, hitting the live agency endpoint.

    Projects one row per series into the declared OutputConfig schema
    (KEY=code, TITLE=title, METADATA=(agency, dataset_id, dynamic
    ``<DIM>_code`` / ``<DIM>_label`` fields)). The ``@enumerator``
    decorator wraps the returned DataFrame into a
    :class:`Result` with :data:`ENUMERATE_SERIES_OUTPUT` attached so
    ``entries_from_result`` can read the schema. The build script stamps the
    per-dataset namespace on the KEY column before building.

    ``fetch_timeout_s`` bounds the subprocess wall-clock; a timeout or
    other subprocess failure raises :class:`~parsimony_sdmx._isolation.FetchSeriesError`
    which the kernel's publish loop catches per-namespace and reports
    as a failed bundle.
    """
    params = EnumerateSeriesParams(agency=agency, dataset_id=dataset_id)
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

    return _series_frame(records, agency=params.agency.value, dataset_id=params.dataset_id)


def _series_frame(records: list[SeriesRecord], *, agency: str, dataset_id: str) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for record in records:
        row = {
            "code": record.id,
            "title": record.title,
            "agency": agency,
            "dataset_id": dataset_id,
        }
        row.update(_dimension_metadata(record.dimensions))
        rows.append(row)
    return pd.DataFrame(rows)


def _dimension_metadata(dimensions: tuple[DimensionValue, ...]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for dimension in dimensions:
        metadata[f"{dimension.id}_code"] = dimension.code
        if dimension.label:
            metadata[f"{dimension.id}_label"] = dimension.label
    return metadata
