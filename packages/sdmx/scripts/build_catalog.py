"""Build SDMX catalog snapshots.

Indexing policy (centralized in :mod:`parsimony_sdmx.catalog_policy`):

* **Series catalogs** index ``code``, ``title``, plus one field per observed SDMX
  dimension (``FREQ``, ``REF_AREA``, ...). Each indexed field uses a hybrid
  BM25+vector index (value-deduplicated, schema v1).
* **Datasets catalogs** (one per agency) index ``code`` (BM25, for direct
  ``ECB|YC``-style lookup), ``title`` and ``description`` via hybrid indexes.

Agency batch builds derive per-flow dimension manifests from series catalogs
and attach them to the agency's ``sdmx_datasets_<agency>`` catalog so
``sdmx_datasets_search`` can tell agents which structured fields are valid
for the next ``sdmx_series_search`` call.

Operator notes:

* ``--catalog series`` with ``--save-root`` also merges the just-built flow's
  manifest into the local agency dataset snapshot (no push). This keeps
  per-flow iterations consistent without re-running ``--catalog agency``.
* ``--catalog agency`` pushes both per-flow series catalogs and the
  agency's ``sdmx_datasets_<agency>`` catalog at the end.
* ``--catalog datasets`` requires ``--agency`` and builds one agency dataset
  catalog (no series rebuild).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from parsimony.catalog import Catalog

from parsimony_sdmx._isolation import ListDatasetsError, list_datasets
from parsimony_sdmx.catalog_build import (
    build_agency_dataset_entities,
    build_datasets_catalog,
    collect_manifests_from_save_root,
    dataset_code,
    dataset_entities_from_records,
    enrich_dataset_entities,
    manifest_from_saved_series,
    manifest_from_series_entries,
)
from parsimony_sdmx.catalog_policy import discover_dim_codes, sdmx_series_entries, sdmx_series_indexes
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES, AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import LISTING_TIMEOUT_S, datasets_namespace
from parsimony_sdmx.connectors.enumerate_series import enumerate_sdmx_series, series_namespace
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.series_selection import select_series_records

logger = logging.getLogger(__name__)


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
    raw_entries = result.data
    dim_codes = discover_dim_codes(raw_entries)
    manifest = manifest_from_series_entries(raw_entries)
    entries = sdmx_series_entries(raw_entries, dim_codes)
    catalog = Catalog(namespace)
    catalog.set_entities(entries)
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


def _hf_bundle_exists(push_root: str, namespace: str) -> bool:
    """Return whether *namespace* is already present under an HF push root."""

    from huggingface_hub import HfApi
    from parsimony.catalog.urls import parse_catalog_url

    repo_id = parse_catalog_url(push_root.rstrip("/")).root
    path = f"{namespace}/meta.json"
    try:
        info = HfApi().get_paths_info(repo_id, paths=[path], repo_type="dataset")
    except Exception:
        return False
    return bool(info) and info[0].size is not None


async def _publish(catalog: Catalog, *, save_root: str | None, push: str | None, push_root: str | None) -> None:
    save = _save_path(save_root, catalog.name)
    if save is not None:
        await catalog.save(save, builder="packages/sdmx/scripts/build_catalog.py")
    targets: list[str] = []
    if push is not None:
        targets.append(push)
    if push_root is not None:
        targets.append(f"{push_root.rstrip('/')}/{catalog.name}")
    for url in targets:
        for attempt, wait in enumerate((0, 30, 120, 300), start=1):
            if wait:
                await asyncio.sleep(wait)
            try:
                await catalog.save(url, builder="packages/sdmx/scripts/build_catalog.py")
                break
            except Exception:  # noqa: BLE001 — HF 500s are transient; retry then re-raise.
                if attempt == 4:
                    raise
                logger.warning("HF push attempt %d failed for %s; retrying", attempt, url)


async def _publish_datasets_catalog(
    catalog: Catalog,
    *,
    save_root: str | None,
    push: str | None,
    push_root: str | None,
) -> None:
    await _publish(catalog, save_root=save_root, push=push, push_root=push_root)
    logger.info("Published %s catalog with %d entries", catalog.name, len(catalog))


async def build_datasets(*, agency: AgencyId, save_root: str | None = None) -> Catalog:
    local_manifests = await collect_manifests_from_save_root(save_root, agency=agency) if save_root else {}
    namespace = datasets_namespace(agency)
    existing_path = _save_path(save_root, namespace)
    try:
        records = await asyncio.to_thread(list_datasets, agency.value, LISTING_TIMEOUT_S)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {agency.value}: {exc.message}") from exc
    entries = enrich_dataset_entities(dataset_entities_from_records(records), local_manifests)
    catalog = await build_datasets_catalog(entries, agency=agency, existing_path=existing_path)
    if local_manifests:
        logger.info("Enriched %s with %d local dimension manifest(s)", namespace, len(local_manifests))
    return catalog


async def build_one(args: argparse.Namespace) -> Catalog:
    if args.catalog == "datasets":
        if args.agency is None:
            raise ValueError("--agency is required for --catalog datasets")
        catalog = await build_datasets(agency=args.agency, save_root=args.save_root)
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
        entries = await build_agency_dataset_entities(records, {build.dataset_code: build.manifest})
        ds_namespace = datasets_namespace(args.agency)
        ds_catalog = await build_datasets_catalog(
            entries,
            agency=args.agency,
            existing_path=_save_path(args.save_root, ds_namespace),
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
    if not catalog.entities:
        return None
    sample = catalog.entities[0].metadata
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

    ds_namespace = datasets_namespace(args.agency)

    logger.info("Fetching dataset listing for %s...", args.agency.value)
    # Phase 1: full agency dataset catalog (discovery index for all flows).
    ds_catalog = await build_datasets(agency=args.agency, save_root=args.save_root)
    logger.info("Publishing %s to storage...", ds_namespace)
    await _publish_datasets_catalog(
        ds_catalog,
        save_root=args.save_root,
        push=args.push,
        push_root=args.push_root,
    )
    logger.info("Published full %s with %d entries", ds_namespace, len(ds_catalog))

    if args.build_all_series:
        selected = [r for r in records if "$" not in r.dataset_id]
    else:
        selected = select_series_records(args.agency, records)
    explicit_skips = _skip_dataset_ids(args.skip_dataset_id)
    selected = [r for r in selected if r.dataset_id.upper() not in explicit_skips]
    logger.info(
        "Selected %d/%d flows for series catalogs (%s)",
        len(selected),
        len([r for r in records if "$" not in r.dataset_id]),
        args.agency.value,
    )

    parallel = max(1, args.parallel)
    sem = asyncio.Semaphore(parallel)
    built = 0
    skipped = 0
    failed: list[str] = []
    manifests: dict[str, list[dict[str, object]]] = {}

    async def _build_one(record: DatasetRecord) -> None:
        nonlocal built, skipped
        namespace = series_namespace(args.agency, record.dataset_id)
        async with sem:
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
                needs_repush = bool(args.push)
                if args.push_root and not args.no_repush:
                    needs_repush = not _hf_bundle_exists(args.push_root, namespace)
                if needs_repush and (args.push_root or args.push):
                    save = _save_path(args.save_root, namespace)
                    if save is not None:
                        try:
                            catalog = await Catalog.load(save)
                            await _publish(
                                catalog,
                                save_root=None,
                                push=args.push,
                                push_root=args.push_root,
                            )
                            logger.info("Re-pushed %s (%d entries)", catalog.name, len(catalog))
                        except Exception:
                            failed.append(record.dataset_id)
                            logger.exception("Failed to re-push %s", namespace)
                            if not args.keep_going:
                                raise
                return
            try:
                build = await build_series(
                    args.agency,
                    record.dataset_id,
                    fetch_timeout_s=args.fetch_timeout_s,
                )
            except Exception:
                failed.append(record.dataset_id)
                logger.exception("Failed to build %s", namespace)
                if not args.keep_going:
                    raise
                return
            try:
                manifests[build.dataset_code] = build.manifest
                await _publish(build.catalog, save_root=args.save_root, push=None, push_root=args.push_root)
                built += 1
                logger.info("Built %s (%d entries)", build.catalog.name, len(build.catalog))
            except Exception:
                failed.append(record.dataset_id)
                logger.exception("Failed to publish %s", namespace)
                if not args.keep_going:
                    raise

    if args.max_catalogs is not None:
        selected = selected[: args.max_catalogs]

    await asyncio.gather(*[_build_one(record) for record in selected])

    # Phase 3: refresh agency dataset catalog — all flows, manifests only where series exist.
    all_entries = enrich_dataset_entities(dataset_entities_from_records(records), manifests)
    ds_catalog = await build_datasets_catalog(
        all_entries,
        agency=args.agency,
        existing_path=_save_path(args.save_root, ds_namespace),
    )
    await _publish_datasets_catalog(
        ds_catalog,
        save_root=args.save_root,
        push=args.push,
        push_root=args.push_root,
    )
    logger.info(
        "Updated %s: %d flows total, %d with dimension manifests",
        ds_namespace,
        len(ds_catalog),
        len(manifests),
    )

    logger.info("Agency batch complete: built=%d skipped=%d failed=%d", built, skipped, len(failed))
    if failed:
        raise ValueError(f"Failed {args.agency.value} catalog(s): {', '.join(failed)}")


async def build_portfolio(args: argparse.Namespace) -> None:
    """Build dataset + selected series catalogs for every supported agency."""

    for agency in ALL_AGENCIES:
        logger.info("=== Portfolio: %s ===", agency.value)
        agency_args = argparse.Namespace(**{**vars(args), "agency": agency, "catalog": "agency"})
        await build_agency_batch(agency_args)


async def run(args: argparse.Namespace) -> None:
    if args.catalog == "portfolio":
        await build_portfolio(args)
    elif args.catalog == "agency":
        await build_agency_batch(args)
    else:
        await build_one(args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", choices=["datasets", "series", "agency", "portfolio"], default="series")
    parser.add_argument("--agency", type=AgencyId, help="SDMX agency, e.g. ECB, ESTAT, IMF_DATA, WB_WDI.")
    parser.add_argument("--dataset-id", help="SDMX dataset/flow id for a single series catalog.")
    parser.add_argument("--max-catalogs", type=int, help="Limit selected series builds in agency batch.")
    parser.add_argument("--parallel", type=int, default=1, help="Concurrent series catalog builds (default 1).")
    parser.add_argument("--build-all-series", action="store_true", help="Skip finance/macro selection heuristics.")
    parser.add_argument("--fetch-timeout-s", type=float, default=900.0, help="Per-flow series enumeration timeout.")
    parser.add_argument("--resume", action="store_true", help="Skip namespaces already present under --save-root.")
    parser.add_argument(
        "--no-repush",
        action="store_true",
        help="On --resume, skip HF re-upload of existing local snapshots (build new bundles only).",
    )
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
