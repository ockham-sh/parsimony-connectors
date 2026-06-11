"""Build BoJ catalog snapshots (multi-bundle layout).

Layout under ``--save-root`` / ``hf://parsimony-dev/boj/``:

* ``boj_databases`` — one row per statistics database (FM08, IR01, …)
* ``boj_series_<db>`` — series rows for that database only

Indexing follows the connectors-repo policy in ``docs/catalog-operations.md``
(adaptive hybrid below 1,000 unique values per field, else BM25-only).

Typical agent chain: ``boj_databases_search`` → ``boj_series_search(db=...)`` → ``boj_fetch``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from parsimony.catalog.source import entities_from_raw

from parsimony_boj import BOJ_ENUMERATE_OUTPUT, enumerate_boj
from parsimony_boj.catalog_build import (
    DATABASES_NAMESPACE,
    build_databases_catalog,
    build_series_catalog,
    split_enumerated_entries,
)

logger = logging.getLogger(__name__)


def _save_path(root: str | None, namespace: str) -> str | None:
    if root is None:
        return None
    return str(Path(root) / namespace)


def _publish(catalog, *, save_root: str | None, push_root: str | None) -> None:
    save = _save_path(save_root, catalog.name)
    if save is not None:
        catalog.save(save, builder="packages/boj/scripts/build_catalog.py")
    if push_root is not None:
        catalog.save(
            f"{push_root.rstrip('/')}/{catalog.name}",
            builder="packages/boj/scripts/build_catalog.py",
        )


def build_all(*, save_root: str | None, push_root: str | None, db_filter: set[str] | None) -> None:
    result = enumerate_boj()
    entries = entities_from_raw(result, BOJ_ENUMERATE_OUTPUT)
    databases, series_by_db = split_enumerated_entries(entries)
    logger.info(
        "BoJ enumerate split: %d databases, %d series namespaces",
        len(databases),
        len(series_by_db),
    )

    db_catalog = build_databases_catalog(databases)
    _publish(db_catalog, save_root=save_root, push_root=push_root)
    logger.info("Built %s with %d entries", DATABASES_NAMESPACE, len(db_catalog))

    for db_code in sorted(series_by_db):
        if db_filter is not None and db_code.upper() not in db_filter:
            continue
        rows = series_by_db[db_code]
        if not rows:
            continue
        catalog = build_series_catalog(db_code, rows)
        _publish(catalog, save_root=save_root, push_root=push_root)
        logger.info("Built %s with %d entries", catalog.name, len(catalog))


def build_databases_only(*, save_root: str | None, push_root: str | None) -> None:
    result = enumerate_boj()
    entries = entities_from_raw(result, BOJ_ENUMERATE_OUTPUT)
    databases, _ = split_enumerated_entries(entries)
    catalog = build_databases_catalog(databases)
    _publish(catalog, save_root=save_root, push_root=push_root)
    logger.info("Built %s with %d entries", catalog.name, len(catalog))


def build_one_series(
    db_code: str,
    *,
    save_root: str | None,
    push_root: str | None,
) -> None:
    result = enumerate_boj()
    entries = entities_from_raw(result, BOJ_ENUMERATE_OUTPUT)
    _, series_by_db = split_enumerated_entries(entries)
    rows = series_by_db.get(db_code.upper()) or series_by_db.get(db_code)
    if not rows:
        raise ValueError(f"No series rows for db={db_code!r} after enumeration")
    catalog = build_series_catalog(db_code.upper(), rows)
    _publish(catalog, save_root=save_root, push_root=push_root)
    logger.info("Built %s with %d entries", catalog.name, len(catalog))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog",
        choices=("all", "databases", "series"),
        default="all",
        help="Which bundle(s) to build (default: all).",
    )
    parser.add_argument("--db", help="Statistics database code for --catalog series (e.g. FM08).")
    parser.add_argument("--save-root", help="Local root directory for multi-bundle snapshots.")
    parser.add_argument("--push-root", help="HF catalog root, e.g. hf://parsimony-dev/boj.")
    parser.add_argument(
        "--only-db",
        action="append",
        dest="only_dbs",
        help="When building all series catalogs, limit to these DB codes (repeatable).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    db_filter = {d.strip().upper() for d in args.only_dbs or [] if d.strip()} or None

    if args.catalog == "databases":
        build_databases_only(save_root=args.save_root, push_root=args.push_root)
    elif args.catalog == "series":
        if not args.db:
            parser.error("--db is required when --catalog series")
        build_one_series(
            args.db.strip().upper(),
            save_root=args.save_root,
            push_root=args.push_root,
        )
    else:
        build_all(
            save_root=args.save_root,
            push_root=args.push_root,
            db_filter=db_filter,
        )


if __name__ == "__main__":
    main()
