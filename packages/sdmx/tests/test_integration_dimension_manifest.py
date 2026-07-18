"""Live-endpoint integration tests for DSD structure and scoped discovery."""

from __future__ import annotations

import pytest

from parsimony_sdmx.catalog_build import dataset_entity_from_structure
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.providers.registry import get_provider

_LIVE_FLOWS: tuple[tuple[AgencyId, str, str], ...] = (
    (AgencyId.ESTAT, "UNE_RT_M", "M.....DE"),
    (AgencyId.ECB, "EXR", "D.USD.EUR.SP00.A"),
)

_DEFAULT_FETCH_TIMEOUT_S = 240.0


@pytest.mark.integration
@pytest.mark.parametrize(("agency", "dataset_id", "key_pattern"), _LIVE_FLOWS)
def test_live_structure_has_dimensions(agency: AgencyId, dataset_id: str, key_pattern: str) -> None:
    provider = get_provider(agency.value)
    record = provider.fetch_structure(dataset_id)
    entry = dataset_entity_from_structure(record)
    assert entry.metadata["dimensions"]
    assert list(record.dsd_order)
