"""IMF (IMF_DATA) adapter — pure shared flow, no provider-specific quirks."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord, StructureRecord
from parsimony_sdmx.providers.sdmx_client import sdmx_client
from parsimony_sdmx.providers.sdmx_flow import (
    discover_series_keys_flow,
    list_datasets_flow,
    list_series_flow,
    list_structure_flow,
)


@dataclass(frozen=True, slots=True)
class ImfProvider:
    agency_id: str = "IMF_DATA"

    def list_datasets(self) -> Iterator[DatasetRecord]:
        with sdmx_client(self.agency_id) as client:
            yield from list_datasets_flow(client, self.agency_id)

    def fetch_structure(self, dataset_id: str) -> StructureRecord:
        with sdmx_client(self.agency_id) as client:
            return list_structure_flow(client, self.agency_id, dataset_id)

    def discover_series_keys(self, dataset_id: str, partial_key: str) -> list[SeriesRecord]:
        with sdmx_client(self.agency_id) as client:
            return discover_series_keys_flow(client, self.agency_id, dataset_id, partial_key)

    def list_series(self, dataset_id: str) -> Iterator[SeriesRecord]:
        with sdmx_client(self.agency_id) as client:
            yield from list_series_flow(client, self.agency_id, dataset_id)
