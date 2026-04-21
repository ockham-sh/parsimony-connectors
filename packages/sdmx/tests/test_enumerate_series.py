"""Tests for ``enumerate_sdmx_series`` — per-dataset series enumerator.

The namespace is composed per-dataset at publish time by
``parsimony_sdmx.CATALOGS`` / ``RESOLVE_CATALOG``; the enumerator itself
leaves the KEY column's namespace unset and lets the catalog supply its
own ``name`` as the default at ingest time.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.catalog import entries_from_result

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import (
    EnumerateSeriesParams,
    enumerate_sdmx_series,
    series_namespace,
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
async def test_entries_use_catalog_name_as_default_namespace(outputs_root: Path) -> None:
    """The enumerator leaves KEY.namespace unset; ``entries_from_result``
    falls back to the caller-supplied default — which in production is the
    catalog's own ``name`` (composed by :func:`series_namespace`).
    """
    result = await enumerate_sdmx_series.bind_deps(outputs_root=outputs_root)(
        EnumerateSeriesParams(agency=AgencyId.ECB, dataset_id="YC"),
    )
    output_config = enumerate_sdmx_series.output_config
    assert output_config is not None
    table = result.to_table(output_config)

    ns = series_namespace(AgencyId.ECB, "YC")
    entries = entries_from_result(table, namespace=ns)

    assert all(e.namespace == "sdmx_series_ecb_yc" for e in entries)
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_accepts_lowercase_agency_from_resolve_catalog() -> None:
    """When ``RESOLVE_CATALOG`` parses ``sdmx_series_ecb_yc`` it passes
    ``agency="ecb"`` (lowercase). The Pydantic ``before`` validator upcases
    it back to the canonical ``AgencyId.ECB``.
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


def test_series_namespace_lowercases_agency_and_dataset() -> None:
    assert series_namespace(AgencyId.ECB, "YC") == "sdmx_series_ecb_yc"
    assert series_namespace(AgencyId.IMF_DATA, "PGI") == "sdmx_series_imf_data_pgi"


def test_enumerator_output_has_no_namespace_on_key() -> None:
    """Per-dataset namespace is supplied at ingest time via the catalog's
    ``name`` — the enumerator's KEY column itself must carry no namespace.
    """
    output_config = enumerate_sdmx_series.output_config
    assert output_config is not None
    cols = output_config.columns
    key_col = next(c for c in cols if c.role.value == "key")
    assert key_col.namespace is None
