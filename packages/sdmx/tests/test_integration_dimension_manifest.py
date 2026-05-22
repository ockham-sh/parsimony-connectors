"""Live-endpoint integration tests for SDMX dimension manifests.

These tests hit real agency APIs via :func:`enumerate_sdmx_series`, derive
dimension manifests from the returned rows, and assert the manifest is usable
for structured search discovery.

Excluded from the default suite by the ``integration`` marker
(see ``pyproject.toml`` addopts). Run explicitly:

    cd packages/sdmx
    uv run pytest -m integration tests/test_integration_dimension_manifest.py -v

Optional slow coverage (World Bank WDI full sweep):

    uv run pytest -m "integration and slow" tests/test_integration_dimension_manifest.py -v
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from parsimony.catalog import CatalogEntry

from parsimony_sdmx.catalog_build import (
    build_agency_dataset_entries,
    dataset_code,
    enrich_dataset_entries,
)
from parsimony_sdmx.catalog_policy import (
    discover_dim_codes,
    sdmx_dimension_manifest,
    sdmx_series_entries,
    sdmx_series_indexes,
)
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series
from parsimony_sdmx.core.models import DatasetRecord

# Flows verified against live endpoints (May 2026). Dataset IDs are case-sensitive
# for some agencies (Eurostat expects ``UNE_RT_M``, not ``une_rt_m``).
_LIVE_FLOWS: tuple[tuple[AgencyId, str], ...] = (
    (AgencyId.ECB, "EXR"),
    (AgencyId.ESTAT, "UNE_RT_M"),
    (AgencyId.IMF_DATA, "FM"),
)

_DEFAULT_FETCH_TIMEOUT_S = 240.0
_WB_FETCH_TIMEOUT_S = 900.0


@dataclass(frozen=True, slots=True)
class LiveManifestResult:
    agency: AgencyId
    dataset_id: str
    row_count: int
    raw_entries: list[CatalogEntry]
    manifest: list[dict[str, object]]
    dim_codes: list[str]


def _catalog_entries_from_enumeration(agency: AgencyId, dataset_id: str, result) -> list[CatalogEntry]:
    del agency, dataset_id
    entries: list[CatalogEntry] = result.data
    assert entries
    assert all(isinstance(entry, CatalogEntry) for entry in entries)
    return entries


async def _live_manifest(agency: AgencyId, dataset_id: str, *, fetch_timeout_s: float) -> LiveManifestResult:
    result = await enumerate_sdmx_series(
        agency=agency,
        dataset_id=dataset_id,
        fetch_timeout_s=fetch_timeout_s,
    )
    raw_entries = _catalog_entries_from_enumeration(agency, dataset_id, result)
    dim_codes = discover_dim_codes(raw_entries)
    manifest = sdmx_dimension_manifest(raw_entries, dim_codes)
    return LiveManifestResult(
        agency=agency,
        dataset_id=dataset_id,
        row_count=len(result.data),
        raw_entries=raw_entries,
        manifest=manifest,
        dim_codes=dim_codes,
    )


def _assert_manifest_is_searchable(result: LiveManifestResult) -> None:
    assert result.row_count > 0, f"{result.agency.value}/{result.dataset_id} returned zero series rows"
    assert result.dim_codes, f"{result.agency.value}/{result.dataset_id} discovered no dimension ids"
    assert result.manifest, f"{result.agency.value}/{result.dataset_id} produced an empty manifest"
    assert [item["id"] for item in result.manifest] == result.dim_codes

    populated = [item for item in result.manifest if item["values"]]
    assert populated, (
        f"{result.agency.value}/{result.dataset_id} manifest has no sample values; "
        f"dimension ids={result.dim_codes!r}"
    )

    for item in populated:
        dim_id = item["id"]
        assert isinstance(dim_id, str) and dim_id
        values = item["values"]
        assert isinstance(values, list) and values
        for sample in values:
            assert isinstance(sample, dict)
            code = sample.get("code")
            label = sample.get("label")
            assert isinstance(code, str) and code.strip(), f"{dim_id}: missing code in {sample!r}"
            assert isinstance(label, str) and label.strip(), f"{dim_id}: missing label in {sample!r}"


@pytest.mark.integration
@pytest.mark.parametrize(("agency", "dataset_id"), _LIVE_FLOWS)
@pytest.mark.asyncio
async def test_live_endpoint_produces_searchable_dimension_manifest(
    agency: AgencyId,
    dataset_id: str,
) -> None:
    result = await _live_manifest(agency, dataset_id, fetch_timeout_s=_DEFAULT_FETCH_TIMEOUT_S)
    _assert_manifest_is_searchable(result)


@pytest.mark.integration
@pytest.mark.parametrize(("agency", "dataset_id"), _LIVE_FLOWS)
@pytest.mark.asyncio
async def test_live_manifest_matches_series_catalog_indexes(
    agency: AgencyId,
    dataset_id: str,
) -> None:
    """Manifest dimension ids must match the fields indexed for structured search."""

    live = await _live_manifest(agency, dataset_id, fetch_timeout_s=_DEFAULT_FETCH_TIMEOUT_S)
    entries = sdmx_series_entries(live.raw_entries, live.dim_codes)
    indexed_fields = {
        idx.field for idx in sdmx_series_indexes(entries, live.dim_codes) if idx.field != "title"
    }

    assert set(live.dim_codes) == indexed_fields
    assert {item["id"] for item in live.manifest} == indexed_fields


@pytest.mark.integration
@pytest.mark.parametrize(("agency", "dataset_id"), _LIVE_FLOWS)
@pytest.mark.asyncio
async def test_live_manifest_surfaces_through_dataset_catalog_enrichment(
    agency: AgencyId,
    dataset_id: str,
) -> None:
    """End-to-end: live enumeration → manifest → dataset catalog metadata."""

    live = await _live_manifest(agency, dataset_id, fetch_timeout_s=_DEFAULT_FETCH_TIMEOUT_S)
    code = dataset_code(agency.value, dataset_id)
    records = [DatasetRecord(dataset_id=dataset_id, agency_id=agency.value, title=f"{agency.value} {dataset_id}")]
    entries = await build_agency_dataset_entries(records, {code: live.manifest})

    assert len(entries) == 1
    assert entries[0].metadata["dimensions"] == live.manifest

    # Merge path used by build_catalog.py must preserve the manifest on upsert.
    existing = [
        CatalogEntry(
            namespace="sdmx_datasets",
            code=code,
            title="placeholder",
            metadata={"agency": agency.value, "dataset_id": dataset_id},
        )
    ]
    merged = enrich_dataset_entries(existing, {code: live.manifest})
    assert merged[0].metadata["dimensions"] == live.manifest


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.asyncio
async def test_live_wb_wdi_manifest_when_endpoint_responds() -> None:
    """World Bank only exposes ``WDI``; the sweep can exceed several minutes.

    This test is opt-in via the ``slow`` marker. It validates WB when the data
    API responds; environments that block or throttle WB data fetches should
    expect a skip rather than a hard failure.
    """

    try:
        result = await _live_manifest(AgencyId.WB_WDI, "WDI", fetch_timeout_s=_WB_FETCH_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 — environment-dependent upstream behaviour.
        pytest.skip(f"WB WDI live sweep unavailable in this environment: {type(exc).__name__}: {exc}")

    _assert_manifest_is_searchable(result)
