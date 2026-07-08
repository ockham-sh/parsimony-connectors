"""ECB adapter — shared SDMX flow + portal scrape."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

from parsimony_sdmx.core.models import DatasetRecord, StructureRecord
from parsimony_sdmx.io.http import HttpConfig, build_session
from parsimony_sdmx.providers.ecb_portal import scrape_ecb_portal
from parsimony_sdmx.providers.sdmx_client import sdmx_client
from parsimony_sdmx.providers.sdmx_flow import (
    list_datasets_flow,
    list_structure_flow,
)


@dataclass(frozen=True, slots=True)
class EcbProvider:
    agency_id: str = "ECB"
    cache_dir: Path | None = None
    http_config: HttpConfig = field(default_factory=HttpConfig)

    def list_datasets(self) -> Iterator[DatasetRecord]:
        session = build_session(self.http_config)
        try:
            descriptions = scrape_ecb_portal(
                session,
                cache_dir=self.cache_dir,
                http_config=self.http_config,
            )
        finally:
            session.close()

        def decorate(flow_id: str, base_title: str) -> str:
            extra = descriptions.get(flow_id)
            if extra:
                return f"{base_title}. {extra}" if base_title else extra
            return base_title

        with sdmx_client(self.agency_id, self.http_config) as client:
            yield from list_datasets_flow(client, self.agency_id, decorate_title=decorate)

    def fetch_structure(self, dataset_id: str) -> StructureRecord:
        with sdmx_client(self.agency_id, self.http_config) as client:
            return list_structure_flow(client, self.agency_id, dataset_id)
