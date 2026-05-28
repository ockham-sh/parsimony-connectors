"""Resolve catalog snapshot metadata for validation (file or in-memory)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from parsimony.catalog.storage import SCHEMA_VERSION, CatalogMeta, read_meta
from parsimony.catalog.urls import parse_catalog_url

if TYPE_CHECKING:
    from parsimony.catalog import Catalog


def _snapshot_path(catalog_url: str) -> Path | None:
    parsed = parse_catalog_url(catalog_url)
    if parsed.scheme != "file":
        return None
    return Path(parsed.root) / parsed.sub if parsed.sub else Path(parsed.root)


def index_fields_from_catalog(catalog: Catalog) -> dict[str, str]:
    """Build ``meta.index_fields`` from a loaded :class:`Catalog`."""
    return {field: index.kind for field, index in catalog.indexes.items()}


def snapshot_meta_for(catalog: Catalog, catalog_url: str) -> CatalogMeta:
    """Return snapshot manifest metadata for *catalog* at *catalog_url*."""
    path = _snapshot_path(catalog_url)
    if path is not None and (path / "meta.json").exists():
        return read_meta(path)
    return CatalogMeta(
        schema_version=SCHEMA_VERSION,
        name=catalog.name,
        namespaces=[],
        entry_count=len(catalog),
        index_fields=index_fields_from_catalog(catalog),
        default_field=catalog.default_field,
    )
