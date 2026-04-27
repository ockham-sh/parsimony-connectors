"""Immutable record types written to parquet."""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DatasetRecord:
    dataset_id: str
    agency_id: str
    title: str


@dataclass(frozen=True, slots=True)
class SeriesRecord:
    id: str
    dataset_id: str
    title: str
    fragments: tuple[str, ...] = field(default_factory=tuple)
