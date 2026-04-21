"""``enumerate_sdmx_datasets`` — catalog enumerator for SDMX dataset discovery.

Reads every ``outputs/{AGENCY}/datasets.parquet`` file produced by the
flat-catalog pipeline and yields one row per ``(agency, dataset_id)``.

Agents consume this via ``Catalog.search(query, namespaces=["sdmx_datasets"])``
after the HF Parquet+FAISS bundle has been loaded (or this enumerator runs
live as fallback when the bundle is missing).

Publish wiring: the plugin module exports
``CATALOGS = [("sdmx_datasets", enumerate_sdmx_datasets), ...]`` (see
``parsimony_sdmx/__init__.py``), which is what ``parsimony publish`` reads.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from parsimony.connector import enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel

from parsimony_sdmx.connectors._agencies import ALL_AGENCIES

#: Catalog namespace for the single cross-agency dataset bundle.
DATASETS_NAMESPACE = "sdmx_datasets"

#: Default root for flat-catalog parquet outputs. Overridden in tests via the
#: ``outputs_root`` dep so we don't tie a live enumerator to a fixed path.
DEFAULT_OUTPUTS_ROOT = Path(__file__).resolve().parent.parent.parent / "outputs"


class EnumerateDatasetsParams(BaseModel):
    """No parameters — the enumerator walks every agency under ``outputs_root``."""


ENUMERATE_DATASETS_OUTPUT = OutputConfig(
    columns=[
        # Composite key; lowercased by normalize_entity_code's contract (it
        # strips but preserves case, so callers should pass the canonical form).
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


def _read_agency_datasets(outputs_root: Path, agency: str) -> pd.DataFrame:
    """Read ``outputs/{AGENCY}/datasets.parquet`` for a single agency.

    Returns an empty DataFrame if the file is absent — not every agency is
    always built locally; downstream concatenation tolerates missing agencies.
    """
    path = outputs_root / agency / "datasets.parquet"
    if not path.exists():
        return pd.DataFrame(columns=["dataset_id", "agency_id", "title"])
    table = pq.read_table(path, columns=["dataset_id", "agency_id", "title"])
    return table.to_pandas()


@enumerator(
    output=ENUMERATE_DATASETS_OUTPUT,
    tags=["sdmx"],
)
async def enumerate_sdmx_datasets(
    params: EnumerateDatasetsParams,
    *,
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT,
) -> pd.DataFrame:
    """List every SDMX dataset from the flat-catalog parquet outputs.

    Walks every agency in :data:`ALL_AGENCIES`, reads its
    ``datasets.parquet``, and returns rows shaped for the kernel's
    ``Catalog.add_from_result`` ingest path — KEY + TITLE + METADATA columns
    only, no observation DATA. The ``@enumerator`` decorator attaches
    :data:`ENUMERATE_DATASETS_OUTPUT` to the resulting :class:`Result` so
    ``catalog.add_from_result()`` can read the schema.

    The composite KEY ``code`` is ``"{agency}|{dataset_id}"`` so agents can
    round-trip it back into :func:`sdmx_fetch` without reconstructing fields.
    """
    frames: list[pd.DataFrame] = []
    for agency in ALL_AGENCIES:
        df = _read_agency_datasets(outputs_root, agency.value)
        if df.empty:
            continue
        df = df.assign(
            agency=df["agency_id"].astype(str),
            code=df["agency_id"].astype(str) + "|" + df["dataset_id"].astype(str),
        )
        frames.append(df[["code", "title", "agency", "dataset_id"]])

    if not frames:
        raise EmptyDataError(
            provider="sdmx",
            message=f"No datasets.parquet found under {outputs_root}; build catalogs first.",
        )

    return pd.concat(frames, ignore_index=True)
