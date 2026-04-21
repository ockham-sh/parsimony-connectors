"""``enumerate_sdmx_series`` — catalog enumerator for per-dataset series discovery.

The per-dataset namespace (``sdmx_series_{agency_lower}_{dataset_id_lower}``)
is composed at publish time by ``parsimony_sdmx.CATALOGS`` / ``RESOLVE_CATALOG``;
this enumerator itself takes ``(agency, dataset_id)`` as params and returns
a :class:`Result` with no namespace on its KEY column — the catalog's name
supplies the default namespace at ingest time (see
``Catalog.add_from_result``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pandas as pd
import pyarrow.parquet as pq
from parsimony.connector import enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field, field_validator

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import DEFAULT_OUTPUTS_ROOT


def series_namespace(agency: AgencyId | str, dataset_id: str) -> str:
    """Compose the canonical per-dataset series namespace string.

    ``AgencyId.ECB`` + ``"YC"`` → ``"sdmx_series_ecb_yc"``. Used by
    ``parsimony_sdmx.CATALOGS`` when yielding catalog entries and by
    ``parsimony_sdmx.RESOLVE_CATALOG`` when round-tripping.
    """
    raw = agency.value if isinstance(agency, AgencyId) else str(agency)
    return f"sdmx_series_{raw.lower()}_{dataset_id.lower()}"


class EnumerateSeriesParams(BaseModel):
    """Parameters for per-dataset series enumeration.

    Accepts both uppercase (``"ECB"``) and lowercase (``"ecb"``) agency
    inputs so round-tripping through ``RESOLVE_CATALOG`` (which hands out
    lowercase tokens parsed from namespace strings) flows through without
    extra casing gymnastics on the caller side.
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
        # KEY namespace is deliberately unset — the catalog's own `name`
        # becomes the default namespace at ingest time, letting one enumerator
        # feed many per-dataset catalogs without templating.
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


def _read_series_parquet(outputs_root: Path, agency: AgencyId, dataset_id: str) -> pd.DataFrame:
    """Read ``outputs/{AGENCY}/series/{dataset_id}.parquet`` for one dataset."""
    path = outputs_root / agency.value / "series" / f"{dataset_id}.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["id", "dataset_id", "title"])
    table = pq.read_table(path, columns=["id", "dataset_id", "title"])
    return table.to_pandas()


@enumerator(
    output=ENUMERATE_SERIES_OUTPUT,
    tags=["sdmx"],
)
async def enumerate_sdmx_series(
    params: EnumerateSeriesParams,
    *,
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT,
) -> pd.DataFrame:
    """List every series inside one SDMX dataset from the flat-catalog parquet.

    Returns rows projecting into the declared OutputConfig schema
    (KEY=code, TITLE=title, METADATA=(agency, dataset_id)); the
    ``@enumerator`` decorator wraps the DataFrame into a :class:`Result`
    with :data:`ENUMERATE_SERIES_OUTPUT` attached so
    ``catalog.add_from_result()`` can read the schema. The catalog name
    (set at publish time by :data:`parsimony_sdmx.CATALOGS`) becomes the
    namespace at ingest time — the KEY column itself carries no namespace.
    """
    df = _read_series_parquet(outputs_root, params.agency, params.dataset_id)
    if df.empty:
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"No series parquet for {params.agency.value}/{params.dataset_id} "
                f"under {outputs_root}; build the catalog first."
            ),
        )

    return pd.DataFrame(
        {
            "code": df["id"].astype(str),
            "title": df["title"].astype(str),
            "agency": params.agency.value,
            "dataset_id": params.dataset_id,
        }
    )
