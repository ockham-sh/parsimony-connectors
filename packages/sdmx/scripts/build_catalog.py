"""Build SDMX catalog snapshots.

Indexing policy (centralized in :mod:`parsimony_sdmx.catalog_policy`):

* **Series catalogs** index ``title`` plus one field per observed SDMX
  dimension (``FREQ``, ``REF_AREA``, ...). Each field uses a hybrid
  BM25+vector index when unique non-empty text count is below
  ``HYBRID_UNIQUE_VALUE_LIMIT`` (100k), otherwise BM25 only.
* **Datasets catalog** (cross-agency) indexes ``code`` (BM25, for direct
  ``ECB|YC``-style lookup), ``title`` and ``description``; the latter two
  follow the same hybrid-or-BM25 cardinality rule.

Agency batch builds derive per-flow dimension manifests from series catalogs
and attach them to the cross-agency ``sdmx_datasets`` catalog so
``sdmx_datasets_search`` can tell agents which structured fields are valid
for the next ``sdmx_series_search`` call.

Operator notes:

* ``--catalog series`` with ``--save-root`` also merges the just-built flow's
  manifest into the local ``sdmx_datasets`` snapshot (no push). This keeps
  per-flow iterations consistent without re-running ``--catalog agency``.
* ``--catalog agency`` pushes both per-flow series catalogs and the
  aggregated ``sdmx_datasets`` catalog at the end.
* ``--catalog datasets`` only enumerates and pushes the cross-agency
  ``sdmx_datasets`` catalog (no series rebuild).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from parsimony.catalog import Catalog, entries_from_result
from parsimony.result import ColumnRole, Result

from parsimony_sdmx._isolation import ListDatasetsError, list_datasets
from parsimony_sdmx.catalog_build import (
    build_agency_dataset_entries,
    build_datasets_catalog,
    collect_manifests_from_save_root,
    dataset_code,
    enrich_datasets_from_enumeration,
    manifest_from_saved_series,
    manifest_from_series_entries,
)
from parsimony_sdmx.catalog_policy import discover_dim_codes, sdmx_series_entries, sdmx_series_indexes
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import (
    DATASETS_NAMESPACE,
    LISTING_TIMEOUT_S,
    enumerate_sdmx_datasets,
)
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series, series_namespace
from parsimony_sdmx.core.models import DatasetRecord

logger = logging.getLogger(__name__)


def _with_key_namespace(result: Result, namespace: str) -> Result:
    if result.output_schema is None:
        raise ValueError("SDMX catalog enumerators must return a Result with output_schema")
    columns = [
        column.model_copy(update={"namespace": namespace}) if column.role == ColumnRole.KEY else column
        for column in result.output_schema.columns
    ]
    return result.model_copy(update={"output_schema": result.output_schema.model_copy(update={"columns": columns})})


@dataclass(frozen=True, slots=True)
class SeriesBuildResult:
    catalog: Catalog
    manifest: list[dict[str, object]]
    dataset_code: str


async def build_series(
    agency: AgencyId,
    dataset_id: str,
    *,
    fetch_timeout_s: float,
) -> SeriesBuildResult:
    namespace = series_namespace(agency, dataset_id)
    result = await enumerate_sdmx_series(agency=agency, dataset_id=dataset_id, fetch_timeout_s=fetch_timeout_s)
    raw_entries = entries_from_result(_with_key_namespace(result, namespace))
    dim_codes = discover_dim_codes(raw_entries)
    manifest = manifest_from_series_entries(raw_entries)
    entries = sdmx_series_entries(raw_entries, dim_codes)
    catalog = Catalog(namespace)
    catalog.set_entries(entries)
    catalog.set_indexes(sdmx_series_indexes(entries, dim_codes))
    await catalog.build()
    return SeriesBuildResult(
        catalog=catalog,
        manifest=manifest,
        dataset_code=dataset_code(agency.value, dataset_id),
    )


def _save_path(root: str | None, namespace: str) -> str | None:
    if root is None:
        return None
    return str(Path(root) / namespace)


def _saved_snapshot_exists(root: str | None, namespace: str) -> bool:
    save = _save_path(root, namespace)
    return save is not None and (Path(save) / "meta.json").exists()


def _skip_dataset_ids(raw: list[str] | None) -> set[str]:
    return {item.strip().upper() for item in raw or [] if item.strip()}


async def _publish(catalog: Catalog, *, save_root: str | None, push: str | None, push_root: str | None) -> None:
    save = _save_path(save_root, catalog.name)
    if save is not None:
        await catalog.save(save, builder="packages/sdmx/scripts/build_catalog.py")
    if push is not None:
        await catalog.save(push, builder="packages/sdmx/scripts/build_catalog.py")
    if push_root is not None:
        await catalog.save(f"{push_root.rstrip('/')}/{catalog.name}", builder="packages/sdmx/scripts/build_catalog.py")


async def _publish_datasets_catalog(
    catalog: Catalog,
    *,
    save_root: str | None,
    push: str | None,
    push_root: str | None,
) -> None:
    await _publish(catalog, save_root=save_root, push=push, push_root=push_root)
    logger.info("Published %s catalog with %d entries", catalog.name, len(catalog))


async def build_datasets(*, save_root: str | None = None) -> Catalog:
    local_manifests = await collect_manifests_from_save_root(save_root) if save_root else {}
    existing_path = _save_path(save_root, DATASETS_NAMESPACE)
    result = await enumerate_sdmx_datasets()
    catalog = await enrich_datasets_from_enumeration(
        result,
        local_manifests,
        existing_path=existing_path,
    )
    if local_manifests:
        logger.info("Enriched datasets catalog with %d local dimension manifest(s)", len(local_manifests))
    return catalog


async def build_one(args: argparse.Namespace) -> Catalog:
    if args.catalog == "datasets":
        catalog = await build_datasets(save_root=args.save_root)
        logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
        await _publish(catalog, save_root=args.save_root, push=args.push, push_root=args.push_root)
        return catalog

    if args.agency is None or args.dataset_id is None:
        raise ValueError("--agency and --dataset-id are required for --catalog series")
    build = await build_series(
        args.agency,
        args.dataset_id,
        fetch_timeout_s=args.fetch_timeout_s,
    )
    logger.info("Built %s catalog with %d entries", build.catalog.name, len(build.catalog))
    await _publish(build.catalog, save_root=args.save_root, push=args.push, push_root=args.push_root)

    if args.save_root is not None:
        try:
            records = await asyncio.to_thread(list_datasets, args.agency.value, LISTING_TIMEOUT_S)
        except ListDatasetsError:
            records = [
                DatasetRecord(
                    dataset_id=args.dataset_id,
                    agency_id=args.agency.value,
                    title=args.dataset_id,
                )
            ]
        else:
            records = [r for r in records if r.dataset_id == args.dataset_id] or [
                DatasetRecord(
                    dataset_id=args.dataset_id,
                    agency_id=args.agency.value,
                    title=args.dataset_id,
                )
            ]
        entries = await build_agency_dataset_entries(records, {build.dataset_code: build.manifest})
        ds_catalog = await build_datasets_catalog(
            entries,
            existing_path=_save_path(args.save_root, DATASETS_NAMESPACE),
        )
        await _publish_datasets_catalog(
            ds_catalog,
            save_root=args.save_root,
            push=None,
            push_root=None,
        )
    return build.catalog


async def _manifest_for_namespace(
    *,
    save_root: str | None,
    namespace: str,
    build: SeriesBuildResult | None,
) -> tuple[str, list[dict[str, object]]] | None:
    if build is not None:
        return build.dataset_code, build.manifest
    save = _save_path(save_root, namespace)
    if save is None or not _saved_snapshot_exists(save_root, namespace):
        return None
    manifest = await manifest_from_saved_series(save)
    catalog = await Catalog.load(save)
    if not catalog.entries:
        return None
    sample = catalog.entries[0].metadata
    agency_id = str(sample.get("agency", "")).strip()
    dataset_id = str(sample.get("dataset_id", "")).strip()
    if not agency_id or not dataset_id:
        logger.warning("Saved series snapshot %s missing agency/dataset_id metadata", namespace)
        return None
    return dataset_code(agency_id, dataset_id), manifest


async def build_agency_batch(args: argparse.Namespace) -> None:
    if args.agency is None:
        raise ValueError("--agency is required for --catalog agency")
    try:
        records = await asyncio.to_thread(list_datasets, args.agency.value, LISTING_TIMEOUT_S)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {args.agency.value}: {exc.message}") from exc

    built = 0
    skipped = 0
    failed: list[str] = []
    manifests: dict[str, list[dict[str, object]]] = {}
    explicit_skips = _skip_dataset_ids(args.skip_dataset_id)
    for record in records:
        if "$" in record.dataset_id:
            continue
        if record.dataset_id.upper() in explicit_skips:
            skipped += 1
            logger.info("Skipping %s by request", series_namespace(args.agency, record.dataset_id))
            continue
        namespace = series_namespace(args.agency, record.dataset_id)
        build: SeriesBuildResult | None = None
        if args.resume and _saved_snapshot_exists(args.save_root, namespace):
            skipped += 1
            logger.info("Skipping %s; existing local snapshot found", namespace)
            manifest_result = await _manifest_for_namespace(
                save_root=args.save_root,
                namespace=namespace,
                build=None,
            )
            if manifest_result is not None:
                code, manifest = manifest_result
                manifests[code] = manifest
            continue
        try:
            build = await build_series(
                args.agency,
                record.dataset_id,
                fetch_timeout_s=args.fetch_timeout_s,
            )
        except Exception:  # noqa: BLE001 - operator batch should report the failing flow and continue when requested.
            failed.append(record.dataset_id)
            logger.exception("Failed to build %s", namespace)
            if not args.keep_going:
                raise
            continue
        manifest_result = await _manifest_for_namespace(
            save_root=args.save_root,
            namespace=namespace,
            build=build,
        )
        if manifest_result is not None:
            code, manifest = manifest_result
            manifests[code] = manifest
        await _publish(build.catalog, save_root=args.save_root, push=None, push_root=args.push_root)
        built += 1
        logger.info("Built %s (%d entries)", build.catalog.name, len(build.catalog))
        if args.max_catalogs is not None and built >= args.max_catalogs:
            break

    if manifests:
        entries = await build_agency_dataset_entries(records, manifests)
        ds_catalog = await build_datasets_catalog(
            entries,
            existing_path=_save_path(args.save_root, DATASETS_NAMESPACE),
        )
        await _publish_datasets_catalog(
            ds_catalog,
            save_root=args.save_root,
            push=args.push,
            push_root=args.push_root,
        )
        logger.info(
            "Updated %s with dimension manifests for %d/%d agency flow(s)",
            DATASETS_NAMESPACE,
            len(manifests),
            len([r for r in records if "$" not in r.dataset_id]),
        )
    else:
        logger.warning(
            "No dimension manifests collected for %s; %s catalog was not updated",
            args.agency.value,
            DATASETS_NAMESPACE,
        )

    logger.info("Agency batch complete: built=%d skipped=%d failed=%d", built, skipped, len(failed))
    if failed:
        raise ValueError(f"Failed {args.agency.value} catalog(s): {', '.join(failed)}")


async def run(args: argparse.Namespace) -> None:
    if args.catalog == "agency":
        await build_agency_batch(args)
    else:
        await build_one(args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", choices=["datasets", "series", "agency"], default="series")
    parser.add_argument("--agency", type=AgencyId, help="SDMX agency, e.g. ECB, ESTAT, IMF_DATA, WB_WDI.")
    parser.add_argument("--dataset-id", help="SDMX dataset/flow id for a single series catalog.")
    parser.add_argument("--max-catalogs", type=int, help="Limit agency batch size.")
    parser.add_argument("--fetch-timeout-s", type=float, default=900.0, help="Per-flow series enumeration timeout.")
    parser.add_argument("--resume", action="store_true", help="Skip namespaces already present under --save-root.")
    parser.add_argument("--keep-going", action="store_true", help="Continue agency batches after a flow failure.")
    parser.add_argument("--skip-dataset-id", action="append", help="Dataset id to skip in agency batches.")
    parser.add_argument("--save-root", help="Local directory where namespace subdirectories are written.")
    parser.add_argument("--push", help="Single catalog URL to push.")
    parser.add_argument("--push-root", help="Root catalog URL for namespace subdirectories.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
