"""Agency adapter contract.

The Protocol is intentionally narrow: two methods that each yield an
iterator of records. Adapters are free to own their own sdmx1 client
and HTTP config; the contract makes no promise about lifecycle beyond
"each method returns an iterator the caller drains in one pass".
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord, StructureRecord


@runtime_checkable
class CatalogProvider(Protocol):
    """Produce dataset, structure, and series records for one agency."""

    @property
    def agency_id(self) -> str: ...

    def list_datasets(self) -> Iterator[DatasetRecord]: ...

    def fetch_structure(self, dataset_id: str) -> StructureRecord: ...

    def list_series(self, dataset_id: str) -> Iterator[SeriesRecord]: ...
