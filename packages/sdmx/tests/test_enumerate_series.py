"""Tests for ``enumerate_sdmx_series`` — template-namespace enumerator.

Exercises the kernel's template-namespace support end-to-end: the enumerator
declares a ``{agency}_{dataset_id}`` template, emits rows, and the kernel
resolves them into per-dataset namespaces. The reverse path (``_find_enumerator``
matching a resolved namespace back to this connector with extracted params)
is verified in the parsimony kernel test suite.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parsimony.bundles.lazy_catalog import _find_enumerator
from parsimony.catalog.catalog import entries_from_table_result

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import (
    SERIES_NAMESPACE_TEMPLATE,
    EnumerateSeriesParams,
    enumerate_sdmx_series,
)


def _write_series_parquet(root: Path, agency: str, dataset_id: str, rows: list[tuple[str, str, str]]) -> None:
    series_dir = root / agency / "series"
    series_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [{"id": sid, "dataset_id": did, "title": title} for sid, did, title in rows],
        schema=pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("dataset_id", pa.string(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
            ]
        ),
    )
    pq.write_table(table, series_dir / f"{dataset_id}.parquet")


@pytest.fixture
def outputs_root(tmp_path: Path) -> Path:
    _write_series_parquet(
        tmp_path,
        "ECB",
        "YC",
        [
            ("B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y", "YC", "10y yield"),
            ("B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y", "YC", "2y yield"),
        ],
    )
    _write_series_parquet(
        tmp_path,
        "ESTAT",
        "prc_hicp_manr",
        [("A.CP00.FR", "prc_hicp_manr", "France annual HICP")],
    )
    return tmp_path


@pytest.mark.asyncio
async def test_enumerates_one_dataset(outputs_root: Path) -> None:
    result = await enumerate_sdmx_series.bind_deps(outputs_root=outputs_root)(
        EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="YC"),
    )
    df = result.data
    assert set(df["code"]) == {
        "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
        "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    }
    # Every row carries the routing METADATA referenced by the template.
    assert set(df["agency"]) == {"ECB"}
    assert set(df["dataset_id"]) == {"YC"}


@pytest.mark.asyncio
async def test_per_row_template_resolution_produces_correct_namespace(outputs_root: Path) -> None:
    result = await enumerate_sdmx_series.bind_deps(outputs_root=outputs_root)(
        EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="YC"),
    )
    table = result.to_table(enumerate_sdmx_series.output_config)
    entries = entries_from_table_result(table)

    # Kernel lowercases placeholder values — namespace is snake_case lowercase.
    assert all(e.namespace == "sdmx_series_ecb_yc" for e in entries)
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_accepts_lowercase_agency_from_kernel_reverse_resolution(outputs_root: Path) -> None:
    """When the kernel reverse-resolves ``sdmx_series_ecb_yc``, it passes
    ``agency="ecb"`` (lowercase) to the enumerator. The Pydantic ``before``
    validator upcases it back to the canonical ``AgencyId.ECB``.
    """
    params = EnumerateSeriesParams(agency="ecb", dataset_id="YC")  # type: ignore[arg-type]
    assert params.agency is AgencyId.ECB


@pytest.mark.asyncio
async def test_missing_dataset_raises_emptydata(outputs_root: Path) -> None:
    from parsimony.errors import EmptyDataError

    with pytest.raises(EmptyDataError):
        await enumerate_sdmx_series.bind_deps(outputs_root=outputs_root)(
            EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="NONEXISTENT"),
        )


def test_find_enumerator_reverse_resolves_against_this_connector() -> None:
    """Kernel's ``_find_enumerator`` must locate this enumerator from a
    resolved namespace like ``sdmx_series_ecb_yc`` and extract the routing
    params so the live-fallback invocation can target the right dataset.
    """
    match = _find_enumerator([enumerate_sdmx_series], "sdmx_series_ecb_yc")
    assert match is not None
    conn, extracted = match
    assert conn is enumerate_sdmx_series
    assert extracted == {"agency": "ecb", "dataset_id": "yc"}


def test_enumerator_output_declares_template_namespace() -> None:
    cols = enumerate_sdmx_series.output_config.columns
    key_col = next(c for c in cols if c.role.value == "key")
    assert key_col.namespace == SERIES_NAMESPACE_TEMPLATE
    assert key_col.namespace_is_template is True
    assert key_col.namespace_placeholders == ["agency", "dataset_id"]
