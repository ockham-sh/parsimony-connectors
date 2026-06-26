"""Build SDMX catalog snapshots (DSD structure + deduplicated codelists).

Indexing policy (centralized in :mod:`parsimony_sdmx.catalog_policy`):

* **Dataset catalogs** (one per agency) index ``code``, ``title``, ``description``.
  Each flow carries a summarized ``dsd`` in metadata.
* **Codelist catalogs** (deduplicated per agency) index ``code`` (BM25) and
  ``label`` (hybrid BM25+vector).

Operator notes:

* ``--catalog structure`` fetches DSD+codelists for one flow and writes a
  structure marker under ``sdmx_structure_<agency>_<dataset>``.
* ``--catalog agency`` lists all flows, fetches structure for each, emits
  ``sdmx_datasets_<agency>`` plus ``sdmx_codelist_<agency>_<codelist>`` catalogs.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path

from parsimony.catalog import Catalog

from parsimony_sdmx._isolation import ListDatasetsError, list_datasets
from parsimony_sdmx.catalog_build import (
    accumulate_codelists,
    assert_codelist_namespace_unique,
    build_codelist_catalog,
    build_datasets_catalog,
    build_structure_for_flow,
    dataset_entities_from_records,
    enrich_dataset_entities_with_dsd,
)
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES, AgencyId
from parsimony_sdmx.connectors.enumerate_datasets import LISTING_TIMEOUT_S, datasets_namespace
from parsimony_sdmx.core.models import CodelistRecord, DatasetRecord, StructureRecord
from parsimony_sdmx.io.structure_json import read_structure, write_structure

logger = logging.getLogger(__name__)


def structure_marker_namespace(agency: AgencyId, dataset_id: str) -> str:
    return f"sdmx_structure_{agency.value.lower()}_{dataset_id.lower()}"


def _lite_structure(record: StructureRecord) -> StructureRecord:
    """Drop the full codelists from a retained structure.

    The datasets catalog only needs ``dimensions``/``dsd_order``/``title`` (see
    ``dataset_entity_from_structure``); the full codelists are already merged
    into the deduplicated ``codelists`` bucket and persisted on disk in the
    structure marker. Keeping them on every retained record would duplicate big
    shared codelists (geo, NACE, …) thousands of times for large agencies.
    """
    return replace(record, codelists=())


@dataclass(frozen=True, slots=True)
class StructureBuildResult:
    record: StructureRecord
    marker_namespace: str


def build_structure(
    agency: AgencyId,
    dataset_id: str,
    *,
    fetch_timeout_s: float,
) -> StructureBuildResult:
    record = build_structure_for_flow(agency, dataset_id, fetch_timeout_s=fetch_timeout_s)
    return StructureBuildResult(record=record, marker_namespace=structure_marker_namespace(agency, dataset_id))


def _save_path(root: str | None, namespace: str) -> str | None:
    if root is None:
        return None
    return str(Path(root) / namespace)


def _marker_exists(root: str | None, namespace: str) -> bool:
    save = _save_path(root, namespace)
    return save is not None and (Path(save) / "meta.json").exists()


def _skip_dataset_ids(raw: list[str] | None) -> set[str]:
    return {item.strip().upper() for item in raw or [] if item.strip()}


def _publish(catalog: Catalog, *, save_root: str | None, push: str | None, push_root: str | None) -> None:
    save = _save_path(save_root, catalog.name)
    if save is not None:
        catalog.save(save, builder="packages/sdmx/scripts/build_catalog.py")
    targets: list[str] = []
    if push is not None:
        targets.append(push)
    if push_root is not None:
        targets.append(f"{push_root.rstrip('/')}/{catalog.name}")
    for url in targets:
        for attempt, wait in enumerate((0, 30, 120, 300), start=1):
            if wait:
                time.sleep(wait)
            try:
                catalog.save(url, builder="packages/sdmx/scripts/build_catalog.py")
                break
            except Exception:  # noqa: BLE001
                if attempt == 4:
                    raise
                logger.warning("HF push attempt %d failed for %s; retrying", attempt, url)


def _write_structure_marker(save_root: str, result: StructureBuildResult) -> None:
    marker_dir = Path(save_root) / result.marker_namespace
    marker_dir.mkdir(parents=True, exist_ok=True)
    write_structure(result.record, marker_dir / "structure.json")
    meta = {
        "namespace": result.marker_namespace,
        "agency": result.record.agency_id,
        "dataset_id": result.record.dataset_id,
        "codelist_count": len(result.record.codelists),
    }
    (marker_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")


def _load_structure_markers(save_root: str | None, agency: AgencyId) -> dict[str, StructureRecord]:
    if save_root is None:
        return {}
    root = Path(save_root)
    prefix = f"sdmx_structure_{agency.value.lower()}_"
    out: dict[str, StructureRecord] = {}
    if not root.is_dir():
        return out
    for sub in sorted(root.iterdir()):
        if not sub.is_dir() or not sub.name.startswith(prefix):
            continue
        structure_path = sub / "structure.json"
        if not structure_path.is_file():
            continue
        record = read_structure(structure_path)
        from parsimony_sdmx.catalog_build import dataset_code

        out[dataset_code(record.agency_id, record.dataset_id)] = record
    return out


def _emit_codelist_catalogs(
    codelists: dict[str, CodelistRecord],
    *,
    agency: AgencyId,
    save_root: str | None,
    push: str | None,
    push_root: str | None,
) -> None:
    assert_codelist_namespace_unique(codelists, agency=agency)
    for cl_id, cl_record in sorted(codelists.items()):
        catalog = build_codelist_catalog(agency, cl_id, cl_record.codes)
        _publish(catalog, save_root=save_root, push=push, push_root=push_root)
        logger.info("Built codelist catalog %s (%d codes)", catalog.name, len(catalog))


def build_datasets(
    *,
    agency: AgencyId,
    save_root: str | None = None,
    structures: dict[str, StructureRecord] | None = None,
) -> Catalog:
    structures = structures or _load_structure_markers(save_root, agency)
    namespace = datasets_namespace(agency)
    existing_path = _save_path(save_root, namespace)
    try:
        records = list_datasets(agency.value, LISTING_TIMEOUT_S)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {agency.value}: {exc.message}") from exc
    entries = dataset_entities_from_records(records)
    if structures:
        entries = enrich_dataset_entities_with_dsd(entries, structures)
    catalog = build_datasets_catalog(entries, agency=agency, existing_path=existing_path)
    if structures:
        logger.info("Enriched %s with %d structure marker(s)", namespace, len(structures))
    return catalog


def build_one(args: argparse.Namespace) -> Catalog:
    if args.catalog == "datasets":
        if args.agency is None:
            raise ValueError("--agency is required for --catalog datasets")
        catalog = build_datasets(agency=args.agency, save_root=args.save_root)
        logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
        _publish(catalog, save_root=args.save_root, push=args.push, push_root=args.push_root)
        return catalog

    if args.agency is None or args.dataset_id is None:
        raise ValueError("--agency and --dataset-id are required for --catalog structure")
    build = build_structure(args.agency, args.dataset_id, fetch_timeout_s=args.fetch_timeout_s)
    if args.save_root is not None:
        _write_structure_marker(args.save_root, build)
    logger.info(
        "Built structure marker %s (%d codelists)",
        build.marker_namespace,
        len(build.record.codelists),
    )
    return Catalog(build.marker_namespace)


def build_agency_batch(args: argparse.Namespace) -> None:
    if args.agency is None:
        raise ValueError("--agency is required for --catalog agency")
    try:
        records = list_datasets(args.agency.value, LISTING_TIMEOUT_S)
    except ListDatasetsError as exc:
        raise ValueError(f"Could not list datasets for {args.agency.value}: {exc.message}") from exc

    selected = [r for r in records if "$" not in r.dataset_id]
    explicit_skips = _skip_dataset_ids(args.skip_dataset_id)
    selected = [r for r in selected if r.dataset_id.upper() not in explicit_skips]
    if args.max_catalogs is not None:
        selected = selected[: args.max_catalogs]

    logger.info("Fetching structure for %d/%d flows (%s)", len(selected), len(records), args.agency.value)

    parallel = max(1, args.parallel)
    sem = threading.Semaphore(parallel)
    state_lock = threading.Lock()
    built = 0
    skipped = 0
    failed: list[str] = []
    codelists: dict[str, CodelistRecord] = {}
    structures: dict[str, StructureRecord] = {}
    resume_cache: dict[str, StructureRecord] = (
        _load_structure_markers(args.save_root, args.agency) if args.resume else {}
    )

    from parsimony_sdmx.catalog_build import dataset_code

    def _record_structure(code: str, record: StructureRecord) -> None:
        with state_lock:
            accumulate_codelists(codelists, record)
            structures[code] = _lite_structure(record)

    def _build_one(record: DatasetRecord) -> None:
        nonlocal built, skipped
        marker_ns = structure_marker_namespace(args.agency, record.dataset_id)
        code = dataset_code(record.agency_id, record.dataset_id)
        with sem:
            if args.resume and _marker_exists(args.save_root, marker_ns):
                cached = resume_cache.get(code)
                if cached is not None:
                    _record_structure(code, cached)
                with state_lock:
                    skipped += 1
                return
            try:
                result = build_structure(args.agency, record.dataset_id, fetch_timeout_s=args.fetch_timeout_s)
            except Exception:
                with state_lock:
                    failed.append(record.dataset_id)
                logger.exception("FAILED structure fetch for %s", record.dataset_id)
                if not args.keep_going:
                    raise
                return
            if args.save_root is not None:
                _write_structure_marker(args.save_root, result)
            _record_structure(code, result.record)
            with state_lock:
                built += 1

    if parallel == 1:
        for record in selected:
            _build_one(record)
    else:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=parallel) as pool:
            list(pool.map(_build_one, selected))

    ds_catalog = build_datasets(agency=args.agency, save_root=args.save_root, structures=structures)
    _publish(ds_catalog, save_root=args.save_root, push=args.push, push_root=args.push_root)
    logger.info("Published %s with %d entries (%d with DSD)", ds_catalog.name, len(ds_catalog), len(structures))

    _emit_codelist_catalogs(
        codelists,
        agency=args.agency,
        save_root=args.save_root,
        push=args.push,
        push_root=args.push_root,
    )

    logger.info(
        "Agency batch complete: built=%d skipped=%d failed=%d codelists=%d",
        built,
        skipped,
        len(failed),
        len(codelists),
    )
    if failed and not args.keep_going:
        raise ValueError(f"Failed structure fetch for: {', '.join(failed)}")


def build_portfolio(args: argparse.Namespace) -> None:
    for agency in ALL_AGENCIES:
        logger.info("=== Portfolio: %s ===", agency.value)
        agency_args = argparse.Namespace(**{**vars(args), "agency": agency, "catalog": "agency"})
        build_agency_batch(agency_args)


def run(args: argparse.Namespace) -> None:
    if args.catalog == "portfolio":
        build_portfolio(args)
    elif args.catalog == "agency":
        build_agency_batch(args)
    else:
        build_one(args)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", choices=["datasets", "structure", "agency", "portfolio"], default="structure")
    parser.add_argument("--agency", type=AgencyId, help="SDMX agency, e.g. ECB, ESTAT, IMF_DATA, WB_WDI.")
    parser.add_argument("--dataset-id", help="SDMX dataset/flow id for a single structure fetch.")
    parser.add_argument("--max-catalogs", type=int, help="Limit structure fetches in agency batch.")
    parser.add_argument("--parallel", type=int, default=1, help="Concurrent structure fetches (default 1).")
    parser.add_argument("--fetch-timeout-s", type=float, default=120.0, help="Per-flow structure fetch timeout.")
    parser.add_argument("--resume", action="store_true", help="Skip flows with existing structure markers.")
    parser.add_argument("--keep-going", action="store_true", help="Continue agency batches after a flow failure.")
    parser.add_argument("--skip-dataset-id", action="append", help="Dataset id to skip in agency batches.")
    parser.add_argument("--save-root", help="Local directory where namespace subdirectories are written.")
    parser.add_argument("--push", help="Single catalog URL to push.")
    parser.add_argument("--push-root", help="Root catalog URL for namespace subdirectories.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args)


if __name__ == "__main__":
    main()
