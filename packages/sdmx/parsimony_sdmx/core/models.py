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


@dataclass(frozen=True, slots=True)
class CodelistCode:
    """One code/label pair from a resolved SDMX codelist."""

    code: str
    label: str


@dataclass(frozen=True, slots=True)
class DimensionStructure:
    """One non-time DSD dimension with codelist reference and sample values."""

    dimension_id: str
    codelist_id: str | None
    name: str | None = None
    code_count: int = 0
    sample: tuple[CodelistCode, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class CodelistRecord:
    """Resolved codes for one SDMX codelist (dedup key = ``codelist_id``)."""

    codelist_id: str
    codes: tuple[CodelistCode, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class StructureRecord:
    """DSD + codelists for one dataflow — no series enumeration."""

    dataset_id: str
    agency_id: str
    title: str
    dsd_order: tuple[str, ...]
    dimensions: tuple[DimensionStructure, ...]
    codelists: tuple[CodelistRecord, ...] = field(default_factory=tuple)
