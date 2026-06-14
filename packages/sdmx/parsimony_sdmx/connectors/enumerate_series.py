"""``enumerate_sdmx_series`` — scoped keys-only series discovery.

Hits the live SDMX agency endpoint for one ``(agency, dataset_id, partial_key)``
and returns matching series keys with labeled dimensions — no observations.
This is the agent feedback loop replacing prebuilt per-flow series catalogs.
"""

from __future__ import annotations

from typing import Annotated

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field, field_validator

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord

MAX_DISCOVERY_RESULTS = 200
_SERIES_KEY_PATTERN = r"^[A-Za-z0-9._+\-]*(?:\.[A-Za-z0-9._+\-]*){0,31}$"


class EnumerateSeriesParams(BaseModel):
    agency: Annotated[AgencyId, Field(description="SDMX source ID (ECB, ESTAT, IMF_DATA, WB_WDI)")]
    dataset_id: Annotated[
        str,
        Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._\-]*$"),
    ]
    key_pattern: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            pattern=_SERIES_KEY_PATTERN,
            description="Partial dot-key in DSD order (empty positions = wildcard).",
        ),
    ]
    limit: int = Field(default=MAX_DISCOVERY_RESULTS, ge=1, le=MAX_DISCOVERY_RESULTS)

    @field_validator("agency", mode="before")
    @classmethod
    def _upcase_agency(cls, v: str | AgencyId) -> str | AgencyId:
        if isinstance(v, str):
            return v.upper()
        return v


ENUMERATE_SERIES_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, description="SDMX series key (dot-separated dimension values)."),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="*", role=ColumnRole.METADATA),
    ]
)


@connector(
    output=ENUMERATE_SERIES_OUTPUT,
    tags=["sdmx"],
)
def enumerate_sdmx_series(
    agency: AgencyId,
    dataset_id: str,
    key_pattern: str,
    limit: int = MAX_DISCOVERY_RESULTS,
) -> pd.DataFrame:
    """Discover matching series for a partial SDMX key (keys-only, no observations).

    Returns labeled dimension metadata columns ``{dim}_code`` / ``{dim}_label``.
    When the upstream result exceeds *limit*, raises :class:`ConnectorError` with a
    per-dimension distinct-value summary so the agent can pin more dimensions.
    """
    params = EnumerateSeriesParams(agency=agency, dataset_id=dataset_id, key_pattern=key_pattern, limit=limit)
    from parsimony_sdmx.providers.registry import get_provider

    try:
        provider = get_provider(params.agency.value)
        records = provider.discover_series_keys(params.dataset_id, params.key_pattern)
    except InvalidParameterError:
        raise
    except Exception as exc:
        raise ConnectorError(
            f"Scoped series discovery failed for {params.agency.value}/{params.dataset_id}: {type(exc).__name__}.",
            provider="sdmx",
        ) from exc

    if not records:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No series match key pattern {params.key_pattern!r} in {params.agency.value}/{params.dataset_id}. "
                "Pin fewer dimensions or verify the key order against the dataset DSD."
            ),
        )

    if len(records) > params.limit:
        summary = _dimension_summary(records)
        raise ConnectorError(
            (
                f"Key pattern {params.key_pattern!r} matched {len(records)} series (limit {params.limit}). "
                f"Pin more dimensions. Distinct values: {summary}"
            ),
            provider="sdmx",
        )

    return _series_frame(records, agency=params.agency.value, dataset_id=params.dataset_id)


def _dimension_summary(records: list[SeriesRecord]) -> str:
    values: dict[str, set[str]] = {}
    for record in records:
        for dim in record.dimensions:
            values.setdefault(dim.id, set()).add(f"{dim.code} ({dim.label or dim.code})")
    parts = [f"{dim_id}: {', '.join(sorted(vals))}" for dim_id, vals in sorted(values.items())]
    return " | ".join(parts)


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


__all__ = ["MAX_DISCOVERY_RESULTS", "enumerate_sdmx_series"]
