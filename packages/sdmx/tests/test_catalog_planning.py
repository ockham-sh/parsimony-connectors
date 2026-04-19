"""Tests for ``parsimony_sdmx._catalog_planning`` (plan generator surface).

Behaviour ported from the deleted ``tests/test_bundles_build.py`` —
specifically the ``discover_series_namespaces_*`` cases. The new pipeline
yields one :class:`CatalogPlan` per ``(agency, dataset_id)`` pair found
on disk; the test fixtures shape parquet files identically to the flat
catalog builder.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.bundles import CatalogPlan

from parsimony_sdmx._catalog_planning import (
    _series_namespace,
    plan_sdmx_series,
)
from parsimony_sdmx.connectors._agencies import AgencyId

# ---------------------------------------------------------------------------
# Namespace composition
# ---------------------------------------------------------------------------


def test_series_namespace_lowercases_agency_and_dataset_id() -> None:
    assert _series_namespace(AgencyId.ECB, "YC") == "sdmx_series_ecb_yc"
    assert _series_namespace(AgencyId.IMF_DATA, "PGI") == "sdmx_series_imf_data_pgi"


# ---------------------------------------------------------------------------
# plan_sdmx_series
# ---------------------------------------------------------------------------


def _write_datasets(root: Path, agency: str, dataset_ids: list[str]) -> None:
    (root / agency).mkdir(parents=True)
    table = pa.Table.from_pylist(
        [{"dataset_id": did, "agency_id": agency, "title": f"title {did}"} for did in dataset_ids],
        schema=pa.schema(
            [
                pa.field("dataset_id", pa.string(), nullable=False),
                pa.field("agency_id", pa.string(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
            ]
        ),
    )
    pq.write_table(table, root / agency / "datasets.parquet")


async def _collect(gen) -> list[CatalogPlan]:
    out: list[CatalogPlan] = []
    async for plan in gen:
        out.append(plan)
    return out


@pytest.mark.asyncio
async def test_plan_sdmx_series_walks_all_agencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_datasets(tmp_path, "ECB", ["YC", "MIR"])
    _write_datasets(tmp_path, "ESTAT", ["prc_hicp_manr"])
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))

    plans = await _collect(plan_sdmx_series())
    namespaces = {p.namespace for p in plans}
    assert namespaces == {
        "sdmx_series_ecb_yc",
        "sdmx_series_ecb_mir",
        "sdmx_series_estat_prc_hicp_manr",
    }


@pytest.mark.asyncio
async def test_plan_sdmx_series_emits_runner_friendly_params(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan params must shape into ``EnumerateSeriesParams`` directly."""
    from parsimony_sdmx.connectors.enumerate_series import EnumerateSeriesParams

    _write_datasets(tmp_path, "ECB", ["YC"])
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))

    plans = await _collect(plan_sdmx_series())
    assert len(plans) == 1
    plan = plans[0]
    # Round-trip via the param model — the runner adapter does this verbatim.
    model = EnumerateSeriesParams(**plan.params)
    assert model.agency is AgencyId.ECB
    assert model.dataset_id == "YC"


@pytest.mark.asyncio
async def test_plan_sdmx_series_empty_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))
    plans = await _collect(plan_sdmx_series())
    assert plans == []


@pytest.mark.asyncio
async def test_plan_sdmx_series_skips_missing_agencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_datasets(tmp_path, "ECB", ["YC"])
    monkeypatch.setenv("PARSIMONY_SDMX_OUTPUTS_ROOT", str(tmp_path))

    plans = await _collect(plan_sdmx_series())
    assert len(plans) == 1
    assert plans[0].namespace == "sdmx_series_ecb_yc"
