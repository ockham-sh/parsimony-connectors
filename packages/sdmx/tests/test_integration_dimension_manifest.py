"""Live-endpoint integration tests for DSD structure and scoped discovery."""

from __future__ import annotations

import pytest

from parsimony_sdmx.catalog_build import dataset_entity_from_structure
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series
from parsimony_sdmx.providers.registry import get_provider

_LIVE_FLOWS: tuple[tuple[AgencyId, str, str], ...] = (
    (AgencyId.ESTAT, "UNE_RT_M", "M.....DE"),
    (AgencyId.ECB, "EXR", "D.USD.EUR.SP00.A"),
)

_DEFAULT_FETCH_TIMEOUT_S = 240.0


@pytest.mark.integration
@pytest.mark.parametrize(("agency", "dataset_id", "key_pattern"), _LIVE_FLOWS)
def test_live_structure_has_dsd_summary(agency: AgencyId, dataset_id: str, key_pattern: str) -> None:
    provider = get_provider(agency.value)
    record = provider.fetch_structure(dataset_id)
    entry = dataset_entity_from_structure(record)
    assert entry.metadata["dsd"]
    assert list(record.dsd_order)
    assert entry.metadata["description"]


@pytest.mark.integration
@pytest.mark.parametrize(("agency", "dataset_id", "key_pattern"), _LIVE_FLOWS)
def test_live_scoped_discovery_returns_labeled_series(
    agency: AgencyId,
    dataset_id: str,
    key_pattern: str,
) -> None:
    df = enumerate_sdmx_series(agency=agency, dataset_id=dataset_id, key_pattern=key_pattern).data
    assert len(df) >= 1
    assert "code" in df.columns
    label_cols = [c for c in df.columns if c.endswith("_label")]
    assert label_cols
