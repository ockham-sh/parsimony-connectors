"""ESTAT (Eurostat) adapter — pure shared flow, no provider-specific quirks."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from parsimony_sdmx.core.models import DatasetRecord, StructureRecord
from parsimony_sdmx.providers.sdmx_client import sdmx_client
from parsimony_sdmx.providers.sdmx_flow import (
    list_datasets_flow,
    list_structure_flow,
)


@dataclass(frozen=True, slots=True)
class EstatProvider:
    agency_id: str = "ESTAT"

    def list_datasets(self) -> Iterator[DatasetRecord]:
        with sdmx_client(self.agency_id) as client:
            yield from list_datasets_flow(client, self.agency_id)

    def fetch_structure(self, dataset_id: str) -> StructureRecord:
        with sdmx_client(self.agency_id) as client:
            return list_structure_flow(client, self.agency_id, dataset_id)
