"""Helpers for assembling SDMX dataset catalogs from DSD structure."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from parsimony.catalog import Catalog, Entity

from parsimony_sdmx._isolation import FetchStructureError, ListDatasetsError, fetch_structure, list_datasets
from parsimony_sdmx.catalog_policy import (
    dsd_summary_from_structure,
    sdmx_datasets_indexes,
)
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.core.models import DatasetRecord, StructureRecord
from parsimony_sdmx.core.namespaces import datasets_namespace

logger = logging.getLogger(__name__)

#: Dataset-listing calls are typically faster than full dataset fetches; cap
#: them tighter so a hung agency-listing doesn't block the others. This is the
#: default fetch timeout for the offline catalog build.
LISTING_TIMEOUT_S: float = 600.0


def dataset_code(agency: str, dataset_id: str) -> str:
    """Return the composite dataset catalog key ``'{agency}|{dataset_id}'``."""
    return f"{agency}|{dataset_id}"


def _agency_from_entries(entries: Sequence[Entity]) -> str:
    agencies = {str(entry.metadata.get("agency", "")).strip() for entry in entries}
    agencies.discard("")
    if len(agencies) != 1:
        raise ValueError(
            f"Dataset catalog entries must belong to exactly one agency; found {sorted(agencies) or 'none'}"
        )
    return agencies.pop()


def datasets_catalog(entries: Sequence[Entity], *, agency: AgencyId | str | None = None) -> Catalog:
    agency_id = agency if agency is not None else _agency_from_entries(entries)
    namespace = datasets_namespace(agency_id)
    return Catalog(
        namespace,
        indexes=sdmx_datasets_indexes(),
        default_field="title",
    )


def dataset_entities_from_records(records: Sequence[DatasetRecord]) -> list[Entity]:
    return [
        Entity(
            namespace=datasets_namespace(record.agency_id),
            code=dataset_code(record.agency_id, record.dataset_id),
            title=record.title,
            metadata={"agency": record.agency_id, "dataset_id": record.dataset_id},
        )
        for record in records
        if "$" not in record.dataset_id
    ]


def dataset_entity_from_structure(record: StructureRecord, *, title: str | None = None) -> Entity:
    dsd = dsd_summary_from_structure(record)
    return Entity(
        namespace=datasets_namespace(record.agency_id),
        code=dataset_code(record.agency_id, record.dataset_id),
        title=title or record.title,
        metadata={
            "agency": record.agency_id,
            "dataset_id": record.dataset_id,
            "dsd": dsd,
            "dsd_order": list(record.dsd_order),
        },
    )


def enrich_dataset_entities_with_dsd(
    entries: Sequence[Entity],
    structures: dict[str, StructureRecord],
) -> list[Entity]:
    """Add DSD metadata to listed entries; never replace their identity.

    The listing title is authoritative — it went through the provider's title
    resolution (e.g. ECB's portal fallback for flows the registry leaves
    unnamed), which the per-flow structure fetch does not.
    """
    out: list[Entity] = []
    for entry in entries:
        structure = structures.get(entry.code)
        if structure is None:
            out.append(entry)
            continue
        out.append(dataset_entity_from_structure(structure, title=entry.title))
    return out


def merge_dataset_entry_lists(
    existing: Sequence[Entity],
    updates: Sequence[Entity],
) -> list[Entity]:
    merged: dict[tuple[str, str], Entity] = {(e.namespace, e.code): e for e in existing}
    for entry in updates:
        merged[(entry.namespace, entry.code)] = entry
    return list(merged.values())


def build_datasets_catalog(
    entries: Sequence[Entity],
    *,
    agency: AgencyId | str | None = None,
    existing_path: str | Path | None = None,
) -> Catalog:
    merged_entries = list(entries)
    if existing_path is not None and Path(existing_path).joinpath("meta.json").exists():
        try:
            existing = Catalog.load(existing_path)
            merged_entries = merge_dataset_entry_lists(existing.entities, entries)
        except Exception:
            logger.warning("Existing datasets snapshot at %s unreadable; rebuilding fresh", existing_path)

    catalog = datasets_catalog(merged_entries, agency=agency)
    catalog.set_entities(merged_entries)
    catalog.build()
    return catalog


def build_agency_datasets_catalog(
    agency: AgencyId,
    *,
    fetch_timeout_s: float = LISTING_TIMEOUT_S,
) -> Catalog:
    try:
        records = list_datasets(agency.value, fetch_timeout_s)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {agency.value}: {exc.message}") from exc
    entries = dataset_entities_from_records(records)
    return build_datasets_catalog(entries, agency=agency)


def build_structure_for_flow(
    agency: AgencyId,
    dataset_id: str,
    *,
    fetch_timeout_s: float = 120.0,
) -> StructureRecord:
    try:
        return fetch_structure(agency.value, dataset_id, fetch_timeout_s)
    except FetchStructureError as exc:
        raise ValueError(f"Structure fetch failed for {agency.value}/{dataset_id}: {exc}") from exc


__all__ = [
    "LISTING_TIMEOUT_S",
    "build_agency_datasets_catalog",
    "build_datasets_catalog",
    "build_structure_for_flow",
    "dataset_code",
    "dataset_entities_from_records",
    "dataset_entity_from_structure",
    "datasets_catalog",
    "enrich_dataset_entities_with_dsd",
    "merge_dataset_entry_lists",
]
