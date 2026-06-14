"""BoJ multi-bundle catalog build helpers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from parsimony.catalog import BM25Index, Catalog, CatalogIndex, Entity
from parsimony.catalog.source import _dataframe_from_raw, entities_from_raw

from parsimony_boj.catalog_policy import adaptive_field_index, discovery_indexes

DATABASES_NAMESPACE = "boj_databases"
DEFAULT_CATALOG_ROOT = "hf://parsimony-dev/boj"
DB_CODE_PREFIX = "db:"


def series_namespace(db_code: str) -> str:
    """Canonical namespace for one BoJ statistics database's series catalog."""

    return f"boj_series_{db_code.strip().lower()}"


def entities_from_boj_enumeration(raw: Any) -> list[Entity]:
    """Project enumerate output to entities, partitioned by database.

    BoJ series codes are unique within a database but may repeat across
    databases with different descriptions; building from the flat combined
    table would violate the entity-metadata invariant. Within a database,
    duplicate codes with conflicting metadata are collapsed (first wins).
    """
    from parsimony_boj import BOJ_ENUMERATE_OUTPUT

    df = _dataframe_from_raw(raw)
    entries: list[Entity] = []

    db_df = df.loc[df["entity_type"] == "db"]
    if not db_df.empty:
        db_df = db_df.drop_duplicates(subset=["code"], keep="first")
        entries.extend(entities_from_raw(db_df.reset_index(drop=True), BOJ_ENUMERATE_OUTPUT))

    series_df = df.loc[df["entity_type"] == "series"]
    for _, group in series_df.groupby("db", sort=True):
        deduped = group.drop_duplicates(subset=["code"], keep="first")
        entries.extend(entities_from_raw(deduped.reset_index(drop=True), BOJ_ENUMERATE_OUTPUT))

    return entries


def _database_entry(entry: Entity) -> Entity:
    if not entry.code.startswith(DB_CODE_PREFIX):
        raise ValueError(f"expected db row code db:<code>, got {entry.code!r}")
    db_code = entry.code[len(DB_CODE_PREFIX) :]
    metadata = dict(entry.metadata)
    metadata["entity_type"] = "db"
    return Entity(
        namespace=DATABASES_NAMESPACE,
        code=db_code,
        title=entry.title,
        metadata=metadata,
    )


def _series_entry(entry: Entity, *, db_code: str) -> Entity:
    return Entity(
        namespace=series_namespace(db_code),
        code=entry.code,
        title=entry.title,
        metadata=dict(entry.metadata),
    )


def split_enumerated_entries(
    entries: Sequence[Entity],
) -> tuple[list[Entity], dict[str, list[Entity]]]:
    """Partition flat ``enumerate_boj`` rows into databases + per-db series lists."""

    databases: list[Entity] = []
    series_by_db: dict[str, list[Entity]] = defaultdict(list)

    for entry in entries:
        entity_type = str(entry.metadata.get("entity_type") or "")
        if entity_type == "db":
            databases.append(_database_entry(entry))
            continue
        if entity_type != "series":
            continue
        db_code = str(entry.metadata.get("db") or "").strip()
        if not db_code:
            continue
        series_by_db[db_code].append(_series_entry(entry, db_code=db_code))

    return databases, dict(series_by_db)


def databases_indexes(entries: Sequence[Entity]) -> dict[str, CatalogIndex]:
    return discovery_indexes(entries, include_description=True)


def series_indexes(entries: Sequence[Entity]) -> dict[str, CatalogIndex]:
    return {
        "code": BM25Index(),
        "title": adaptive_field_index("title", entries),
        "description": adaptive_field_index("description", entries),
    }


def build_databases_catalog(entries: Sequence[Entity]) -> Catalog:
    catalog = Catalog(DATABASES_NAMESPACE, default_field="title")
    catalog.set_entities(list(entries))
    catalog.set_indexes(databases_indexes(entries))
    catalog.build()
    return catalog


def build_series_catalog(db_code: str, entries: Sequence[Entity]) -> Catalog:
    namespace = series_namespace(db_code)
    catalog = Catalog(namespace, default_field="title")
    catalog.set_entities(list(entries))
    catalog.set_indexes(series_indexes(entries))
    catalog.build()
    return catalog


def build_boj_databases_catalog_from_enumeration() -> Catalog:
    """Enumerate all BoJ databases and build the databases catalog."""
    from parsimony_boj import enumerate_boj

    result = enumerate_boj()
    entries = entities_from_boj_enumeration(result)
    databases, _ = split_enumerated_entries(entries)
    return build_databases_catalog(databases)


def build_boj_series_catalog_for_db(db_code: str) -> Catalog:
    """Fetch one BoJ database and build its per-database series catalog."""
    from parsimony_boj import BOJ_ENUMERATE_OUTPUT, fetch_boj_enumeration_rows_for_db

    normalized = db_code.strip().upper()
    df = fetch_boj_enumeration_rows_for_db(normalized)
    entries = entities_from_raw(df, BOJ_ENUMERATE_OUTPUT)
    _, series_by_db = split_enumerated_entries(entries)
    rows = series_by_db.get(normalized) or series_by_db.get(db_code.strip()) or []
    if not rows:
        raise ValueError(f"No series rows for db={db_code!r} after enumeration")
    return build_series_catalog(normalized, rows)


__all__ = [
    "DATABASES_NAMESPACE",
    "DEFAULT_CATALOG_ROOT",
    "build_boj_databases_catalog_from_enumeration",
    "build_boj_series_catalog_for_db",
    "build_databases_catalog",
    "build_series_catalog",
    "databases_indexes",
    "entities_from_boj_enumeration",
    "series_indexes",
    "series_namespace",
    "split_enumerated_entries",
]
