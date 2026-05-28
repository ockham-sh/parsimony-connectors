"""Immutable SDMX record types streamed through the isolation boundary."""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DatasetRecord:
    dataset_id: str
    agency_id: str
    title: str


@dataclass(frozen=True, slots=True)
class DimensionValue:
    """One SDMX dimension value in a series key."""

    id: str
    code: str
    label: str | None = None


@dataclass(frozen=True, slots=True)
class SeriesRecord:
    """One streamed SDMX series row before it becomes a Parsimony Result row."""

    id: str
    dataset_id: str
    title: str
    dimensions: tuple[DimensionValue, ...] = field(default_factory=tuple)
