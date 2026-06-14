"""Helpers for assembling SDMX dataset and codelist catalogs from DSD structure."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from parsimony.catalog import Catalog, Entity, code_token

from parsimony_sdmx._isolation import FetchStructureError, ListDatasetsError, fetch_structure, list_datasets
from parsimony_sdmx.catalog_policy import (
    dsd_description_text,
    dsd_summary_from_structure,
    sdmx_codelist_indexes,
    sdmx_datasets_indexes,
)
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.codelist_namespace import codelist_namespace
from parsimony_sdmx.connectors.enumerate_datasets import LISTING_TIMEOUT_S, datasets_namespace
from parsimony_sdmx.core.models import CodelistCode, CodelistRecord, DatasetRecord, StructureRecord

logger = logging.getLogger(__name__)


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
        indexes=sdmx_datasets_indexes(entries),
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


def dataset_entity_from_structure(record: StructureRecord) -> Entity:
    dsd = dsd_summary_from_structure(record, agency=record.agency_id)
    description = dsd_description_text(record)
    return Entity(
        namespace=datasets_namespace(record.agency_id),
        code=dataset_code(record.agency_id, record.dataset_id),
        title=record.title,
        metadata={
            "agency": record.agency_id,
            "dataset_id": record.dataset_id,
            "description": description,
            "dsd": dsd,
            "dsd_order": list(record.dsd_order),
        },
    )


def codelist_entities(record: CodelistRecord, *, agency: AgencyId | str) -> list[Entity]:
    namespace = codelist_namespace(agency, record.codelist_id)
    return [
        Entity(
            namespace=namespace,
            code=code.code,
            title=code.label,
            metadata={"label": code.label, "codelist_id": record.codelist_id},
        )
        for code in record.codes
    ]


def merge_codelist_records(existing: CodelistRecord | None, incoming: CodelistRecord) -> CodelistRecord:
    if existing is None:
        return incoming
    merged: dict[str, CodelistCode] = {code.code: code for code in existing.codes}
    for code in incoming.codes:
        merged.setdefault(code.code, code)
    return CodelistRecord(
        codelist_id=incoming.codelist_id,
        codes=tuple(sorted(merged.values(), key=lambda item: item.code)),
    )


def accumulate_codelists(
    bucket: dict[str, CodelistRecord],
    record: StructureRecord,
) -> None:
    for cl in record.codelists:
        bucket[cl.codelist_id] = merge_codelist_records(bucket.get(cl.codelist_id), cl)


def assert_codelist_namespace_unique(codelists: dict[str, CodelistRecord], *, agency: AgencyId | str) -> None:
    by_token: dict[str, str] = {}
    for cl_id in codelists:
        token = code_token(cl_id)
        ns = codelist_namespace(agency, cl_id)
        prior = by_token.get(token)
        if prior is not None and prior != cl_id:
            raise ValueError(
                f"Codelist namespace collision for agency {agency}: "
                f"{prior!r} and {cl_id!r} both tokenize to {token!r} ({ns})"
            )
        by_token[token] = cl_id


def build_codelist_catalog(
    agency: AgencyId | str,
    codelist_id: str,
    codes: Sequence[CodelistCode],
) -> Catalog:
    record = CodelistRecord(codelist_id=codelist_id, codes=tuple(codes))
    entries = codelist_entities(record, agency=agency)
    catalog = Catalog(codelist_namespace(agency, codelist_id), default_field="label")
    catalog.set_entities(entries)
    catalog.set_indexes(sdmx_codelist_indexes(entries))
    catalog.build()
    return catalog


def build_codelist_catalog_from_structure(
    agency: AgencyId,
    codelist_id: str,
    *,
    fetch_timeout_s: float = 120.0,
    dataset_id_hint: str | None = None,
) -> Catalog:
    """Lazy-build one codelist catalog from a live structure fetch."""
    if dataset_id_hint is None:
        raise ValueError("dataset_id_hint is required for lazy codelist build")
    record = fetch_structure(agency.value, dataset_id_hint, fetch_timeout_s)
    match = next((cl for cl in record.codelists if cl.codelist_id == codelist_id), None)
    if match is None:
        raise ValueError(f"Codelist {codelist_id!r} not found in structure for {agency.value}/{dataset_id_hint}")
    return build_codelist_catalog(agency, codelist_id, match.codes)


def enrich_dataset_entities_with_dsd(
    entries: Sequence[Entity],
    structures: dict[str, StructureRecord],
) -> list[Entity]:
    out: list[Entity] = []
    for entry in entries:
        structure = structures.get(entry.code)
        if structure is None:
            out.append(entry)
            continue
        out.append(dataset_entity_from_structure(structure))
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
    "accumulate_codelists",
    "assert_codelist_namespace_unique",
    "build_agency_datasets_catalog",
    "build_codelist_catalog",
    "build_codelist_catalog_from_structure",
    "build_datasets_catalog",
    "build_structure_for_flow",
    "codelist_entities",
    "dataset_code",
    "dataset_entities_from_records",
    "dataset_entity_from_structure",
    "datasets_catalog",
    "enrich_dataset_entities_with_dsd",
    "merge_codelist_records",
    "merge_dataset_entry_lists",
]
