"""``enumerate_sdmx_series`` — catalog enumerator for per-dataset series discovery.

Exercises the kernel's template-namespace support: the KEY column declares
``namespace="sdmx_series_{agency}_{dataset_id}"``, and
``Catalog.index_result`` resolves it per row into namespaces like
``sdmx_series_ecb_yc``. On reverse-lookup (``Catalog.search(namespaces=["sdmx_series_ecb_yc"])``)
the kernel extracts ``(agency="ecb", dataset_id="yc")`` from the resolved
namespace and invokes this enumerator with those params — so the Pydantic
model has to accept the lowercased form too.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pandas as pd
import pyarrow.parquet as pq
from parsimony.bundles import CatalogSpec
from parsimony.connector import enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig, Provenance, Result
from pydantic import BaseModel, Field, field_validator

from parsimony_sdmx._catalog_planning import plan_sdmx_series
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import DEFAULT_OUTPUTS_ROOT

#: Template namespace resolved per row.
SERIES_NAMESPACE_TEMPLATE = "sdmx_series_{agency}_{dataset_id}"


class EnumerateSeriesParams(BaseModel):
    """Parameters for per-dataset series enumeration.

    Accepts both uppercase (``"ECB"``) and lowercase (``"ecb"``) agency
    inputs so the kernel's reverse-resolved namespace params (always
    lowercase, per the catalog's normalize_code contract) flow through
    cleanly.
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
            namespace=SERIES_NAMESPACE_TEMPLATE,
            description="SDMX series key (dot-separated dimension values).",
        ),
        Column(name="title", role=ColumnRole.TITLE),
        # Placeholders referenced by the template namespace MUST be declared
        # here for OutputConfig's validator to accept the template.
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
    catalog=CatalogSpec(plan=plan_sdmx_series),
)
async def enumerate_sdmx_series(
    params: EnumerateSeriesParams,
    *,
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT,
) -> Result:
    """List every series inside one SDMX dataset from the flat-catalog parquet.

    The namespace is templated from ``(agency, dataset_id)`` — every row
    lands in ``sdmx_series_{agency}_{dataset_id}`` after kernel-side
    per-row resolution. One enumerator serves thousands of namespaces
    because the kernel matches the resolved form back to this same
    connector via reverse template resolution.
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

    # Project into the declared OutputConfig schema: KEY=code, TITLE=title,
    # METADATA=(agency, dataset_id, series_key). The kernel lowercases template
    # placeholder values before resolving, so we can keep the canonical
    # uppercase agency here.
    out = pd.DataFrame(
        {
            "code": df["id"].astype(str),
            "title": df["title"].astype(str),
            "agency": params.agency.value,
            "dataset_id": params.dataset_id,
        }
    )

    provenance = Provenance(
        source="sdmx",
        params={"agency": params.agency.value, "dataset_id": params.dataset_id},
        properties={"outputs_root": str(outputs_root)},
    )
    return Result.from_dataframe(out, provenance)
