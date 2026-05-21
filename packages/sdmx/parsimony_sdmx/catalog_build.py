"""Helpers for assembling SDMX dataset catalogs with dimension manifests."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

from parsimony.catalog import Catalog, CatalogEntry, entries_from_result

from parsimony_sdmx.catalog_policy import discover_dim_codes, sdmx_datasets_indexes, sdmx_dimension_manifest
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import DATASETS_NAMESPACE
from parsimony_sdmx.core.models import DatasetRecord

logger = logging.getLogger(__name__)


def dataset_code(agency: str, dataset_id: str) -> str:
    """Return the composite dataset catalog key ``'{agency}|{dataset_id}'``."""

    return f"{agency}|{dataset_id}"


def datasets_catalog(entries: Sequence[CatalogEntry]) -> Catalog:
    return Catalog(
        DATASETS_NAMESPACE,
        indexes=sdmx_datasets_indexes(entries),
        default_field="title",
    )


def dataset_entries_from_records(records: Sequence[DatasetRecord]) -> list[CatalogEntry]:
    return [
        CatalogEntry(
            namespace=DATASETS_NAMESPACE,
            code=dataset_code(record.agency_id, record.dataset_id),
            title=record.title,
            metadata={"agency": record.agency_id, "dataset_id": record.dataset_id},
        )
        for record in records
        if "$" not in record.dataset_id
    ]


def enrich_dataset_entries(
    entries: Sequence[CatalogEntry],
    manifests: dict[str, list[dict[str, object]]],
) -> list[CatalogEntry]:
    """Attach ``dimensions`` metadata to dataset entries when a manifest exists."""

    out: list[CatalogEntry] = []
    for entry in entries:
        manifest = manifests.get(entry.code)
        if manifest is None:
            out.append(entry)
            continue
        metadata = dict(entry.metadata)
        metadata["dimensions"] = manifest
        out.append(
            CatalogEntry(
                namespace=entry.namespace,
                code=entry.code,
                title=entry.title,
                metadata=metadata,
            )
        )
    return out


def manifest_from_series_entries(entries: Sequence[CatalogEntry]) -> list[dict[str, object]]:
    dim_codes = discover_dim_codes(entries)
    return sdmx_dimension_manifest(entries, dim_codes)


async def manifest_from_saved_series(path: str | Path) -> list[dict[str, object]]:
    catalog = await Catalog.load(path)
    return manifest_from_series_entries(catalog.entries)


def merge_dataset_entry_lists(
    existing: Sequence[CatalogEntry],
    updates: Sequence[CatalogEntry],
) -> list[CatalogEntry]:
    """Upsert *updates* into *existing* by ``(namespace, code)``."""

    merged: dict[tuple[str, str], CatalogEntry] = {(e.namespace, e.code): e for e in existing}
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
            catalog = await Catalog.load(sub)
        except Exception:  # noqa: BLE001 — skip unreadable snapshots during enrichment scans.
            logger.warning("Skipping unreadable series snapshot at %s", sub)
            continue
        if not catalog.entries:
            continue
        sample = catalog.entries[0].metadata
        agency_id = str(sample.get("agency", "")).strip()
        dataset_id = str(sample.get("dataset_id", "")).strip()
        if not agency_id or not dataset_id:
            logger.warning("Series snapshot %s missing agency/dataset_id metadata", sub.name)
            continue
        code = dataset_code(agency_id, dataset_id)
        manifests[code] = manifest_from_series_entries(catalog.entries)
    return manifests


async def build_datasets_catalog(
    entries: Sequence[CatalogEntry],
    *,
    existing_path: str | Path | None = None,
) -> Catalog:
    """Build the cross-agency datasets catalog, optionally merging with an existing snapshot."""

    merged_entries = list(entries)
    if existing_path is not None and Path(existing_path).joinpath("meta.json").exists():
        existing = await Catalog.load(existing_path)
        merged_entries = merge_dataset_entry_lists(existing.entries, entries)

    catalog = datasets_catalog(merged_entries)
    catalog.set_entries(merged_entries)
    await catalog.build()
    return catalog


async def build_agency_dataset_entries(
    records: Sequence[DatasetRecord],
    manifests: dict[str, list[dict[str, object]]],
) -> list[CatalogEntry]:
    """Build dataset entries only for flows with collected manifests."""

    selected = [
        record
        for record in records
        if "$" not in record.dataset_id and dataset_code(record.agency_id, record.dataset_id) in manifests
    ]
    return enrich_dataset_entries(dataset_entries_from_records(selected), manifests)


async def enrich_datasets_from_enumeration(
    enumeration_result,
    manifests: dict[str, list[dict[str, object]]],
    *,
    existing_path: str | Path | None = None,
) -> Catalog:
    entries = enrich_dataset_entries(entries_from_result(enumeration_result), manifests)
    return await build_datasets_catalog(entries, existing_path=existing_path)


__all__ = [
    "build_agency_dataset_entries",
    "build_datasets_catalog",
    "collect_manifests_from_save_root",
    "dataset_code",
    "dataset_entries_from_records",
    "datasets_catalog",
    "enrich_dataset_entries",
    "enrich_datasets_from_enumeration",
    "manifest_from_saved_series",
    "manifest_from_series_entries",
    "merge_dataset_entry_lists",
]
