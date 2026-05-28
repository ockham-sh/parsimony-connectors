"""Helpers for assembling SDMX dataset catalogs with dimension manifests."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from pathlib import Path

from parsimony.catalog import Catalog, Entity
from parsimony.catalog.source import entities_from_raw
from parsimony.catalog.storage import ENTRIES_FILENAME, _read_parquet

from parsimony_sdmx._isolation import ListDatasetsError, list_datasets
from parsimony_sdmx.catalog_policy import (
    discover_dim_codes,
    sdmx_datasets_indexes,
    sdmx_dimension_manifest,
    sdmx_series_entries,
    sdmx_series_indexes,
)
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import LISTING_TIMEOUT_S, datasets_namespace
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series, series_namespace
from parsimony_sdmx.core.models import DatasetRecord

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


def enrich_dataset_entities(
    entries: Sequence[Entity],
    manifests: dict[str, list[dict[str, object]]],
) -> list[Entity]:
    """Attach ``dimensions`` metadata to dataset entries when a manifest exists."""

    out: list[Entity] = []
    for entry in entries:
        manifest = manifests.get(entry.code)
        if manifest is None:
            out.append(entry)
            continue
        metadata = dict(entry.metadata)
        metadata["dimensions"] = manifest
        out.append(
            Entity(
                namespace=entry.namespace,
                code=entry.code,
                title=entry.title,
                metadata=metadata,
            )
        )
    return out


def manifest_from_series_entries(entries: Sequence[Entity]) -> list[dict[str, object]]:
    dim_codes = discover_dim_codes(entries)
    return sdmx_dimension_manifest(entries, dim_codes)


async def manifest_from_saved_series(path: str | Path) -> list[dict[str, object]]:
    entries = await asyncio.to_thread(_read_parquet, Path(path) / ENTRIES_FILENAME)
    return manifest_from_series_entries(entries)


def merge_dataset_entry_lists(
    existing: Sequence[Entity],
    updates: Sequence[Entity],
) -> list[Entity]:
    """Upsert *updates* into *existing* by ``(namespace, code)``."""

    merged: dict[tuple[str, str], Entity] = {(e.namespace, e.code): e for e in existing}
    for entry in updates:
        merged[(entry.namespace, entry.code)] = entry
    return list(merged.values())


async def collect_manifests_from_save_root(
    save_root: str | Path,
    *,
    agency: AgencyId | None = None,
) -> dict[str, list[dict[str, object]]]:
    """Scan local series snapshots and derive dataset-code → manifest mappings."""

    root = Path(save_root)
    if not root.is_dir():
        return {}

    agency_prefix = f"sdmx_series_{agency.value.lower()}_" if agency is not None else "sdmx_series_"
    manifests: dict[str, list[dict[str, object]]] = {}

    for sub in sorted(root.iterdir()):
        if not sub.is_dir() or not sub.name.startswith(agency_prefix):
            continue
        if not (sub / "meta.json").exists():
            continue
        try:
            entries = await asyncio.to_thread(_read_parquet, sub / ENTRIES_FILENAME)
        except Exception:  # noqa: BLE001 — skip unreadable snapshots during enrichment scans.
            logger.warning("Skipping unreadable series snapshot at %s", sub)
            continue
        if not entries:
            continue
        sample = entries[0].metadata
        agency_id = str(sample.get("agency", "")).strip()
        dataset_id = str(sample.get("dataset_id", "")).strip()
        if not agency_id or not dataset_id:
            logger.warning("Series snapshot %s missing agency/dataset_id metadata", sub.name)
            continue
        code = dataset_code(agency_id, dataset_id)
        manifests[code] = manifest_from_series_entries(entries)
    return manifests


async def build_datasets_catalog(
    entries: Sequence[Entity],
    *,
    agency: AgencyId | str | None = None,
    existing_path: str | Path | None = None,
) -> Catalog:
    """Build a per-agency datasets catalog, optionally merging with an existing snapshot."""

    merged_entries = list(entries)
    if existing_path is not None and Path(existing_path).joinpath("meta.json").exists():
        existing = await Catalog.load(existing_path)
        merged_entries = merge_dataset_entry_lists(existing.entities, entries)

    catalog = datasets_catalog(merged_entries, agency=agency)
    catalog.set_entities(merged_entries)
    await catalog.build()
    return catalog


async def build_agency_dataset_entities(
    records: Sequence[DatasetRecord],
    manifests: dict[str, list[dict[str, object]]],
) -> list[Entity]:
    """Build dataset entries only for flows with collected manifests."""

    selected = [
        record
        for record in records
        if "$" not in record.dataset_id and dataset_code(record.agency_id, record.dataset_id) in manifests
    ]
    return enrich_dataset_entities(dataset_entities_from_records(selected), manifests)


async def enrich_datasets_from_enumeration(
    enumeration_result,
    manifests: dict[str, list[dict[str, object]]],
    *,
    agency: AgencyId | str,
    existing_path: str | Path | None = None,
) -> Catalog:
    agency_id = agency.value if isinstance(agency, AgencyId) else str(agency)
    from parsimony_sdmx.connectors.enumerate_datasets import SDMX_DATASETS_ENUM_OUTPUT

    all_entries = entities_from_raw(enumeration_result, SDMX_DATASETS_ENUM_OUTPUT)
    agency_entries = [
        entry for entry in all_entries if str(entry.metadata.get("agency", "")).strip() == agency_id
    ]
    entries = enrich_dataset_entities(agency_entries, manifests)
    return await build_datasets_catalog(entries, agency=agency, existing_path=existing_path)


async def build_series_catalog(
    agency: AgencyId,
    dataset_id: str,
    *,
    fetch_timeout_s: float = 900.0,
) -> Catalog:
    """Build one per-flow SDMX series catalog from live enumeration."""
    from parsimony_sdmx.connectors.enumerate_series import _series_output_config

    result = await enumerate_sdmx_series(agency=agency, dataset_id=dataset_id, fetch_timeout_s=fetch_timeout_s)
    schema = _series_output_config(agency, dataset_id)
    raw_entries = entities_from_raw(result, schema)
    dim_codes = discover_dim_codes(raw_entries)
    entries = sdmx_series_entries(raw_entries, dim_codes)
    catalog = Catalog(series_namespace(agency, dataset_id))
    catalog.set_entities(entries)
    catalog.set_indexes(sdmx_series_indexes(entries, dim_codes))
    await catalog.build()
    return catalog


async def build_agency_datasets_catalog(
    agency: AgencyId,
    *,
    fetch_timeout_s: float = LISTING_TIMEOUT_S,
) -> Catalog:
    """Build the per-agency SDMX datasets discovery catalog from live listing."""
    try:
        records = await asyncio.to_thread(list_datasets, agency.value, fetch_timeout_s)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {agency.value}: {exc.message}") from exc
    entries = dataset_entities_from_records(records)
    return await build_datasets_catalog(entries, agency=agency)


__all__ = [
    "build_agency_dataset_entities",
    "build_agency_datasets_catalog",
    "build_datasets_catalog",
    "build_series_catalog",
    "collect_manifests_from_save_root",
    "dataset_code",
    "dataset_entities_from_records",
    "datasets_catalog",
    "enrich_dataset_entities",
    "enrich_datasets_from_enumeration",
    "manifest_from_saved_series",
    "manifest_from_series_entries",
    "merge_dataset_entry_lists",
]
