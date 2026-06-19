#!/usr/bin/env python3
"""Isolated subprocess worker for one SDMX series catalog task."""

from __future__ import annotations

import argparse
import json
import logging
import os
import resource
import sys
from pathlib import Path

# Limit BLAS/torch threads before heavy imports in index mode.
os.environ.setdefault("OMP_NUM_THREADS", "2")
os.environ.setdefault("MKL_NUM_THREADS", "2")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "2")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

from parsimony_sdmx.catalog_manifest import BuildRoot
from parsimony_sdmx.catalog_series import (
    build_flow_catalog,
    build_series_parquet,
    collect_distinct_from_columnar,
    load_structure_marker,
    series_namespace,
)
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.io.http import HttpConfig, build_session
from parsimony_sdmx.series_fields import SERIES_PARQUET

logger = logging.getLogger(__name__)

FETCH_RLIMIT_GB = 2
INDEX_RLIMIT_GB = 5


def _set_memory_limit(gb: int) -> None:
    limit = gb * 1024 * 1024 * 1024
    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (OSError, ValueError):
        logger.warning("Could not set RLIMIT_AS to %d GB on this platform", gb)


def _series_staging_dir(layout: BuildRoot, namespace: str) -> Path:
    return layout.staging / "series" / namespace


def _fetch_meta_path(layout: BuildRoot, namespace: str) -> Path:
    return _series_staging_dir(layout, namespace) / "fetch_meta.json"


def run_series_fetch(layout: BuildRoot, agency: AgencyId, flow_id: str) -> dict:
    _set_memory_limit(FETCH_RLIMIT_GB)
    namespace = series_namespace(agency, flow_id)
    staging = _series_staging_dir(layout, namespace)
    staging.mkdir(parents=True, exist_ok=True)
    parquet_path = staging / SERIES_PARQUET
    structure = load_structure_marker(layout.catalogs, agency, flow_id)
    cfg = HttpConfig(read_timeout=600.0, max_response_bytes=800 * 1024 * 1024)
    session = build_session(cfg)
    try:
        count = build_series_parquet(
            agency,
            flow_id,
            structure=structure,
            out_parquet=parquet_path,
            session=session,
            http_config=cfg,
        )
    finally:
        session.close()
    meta = {"namespace": namespace, "agency": agency.value, "flow_id": flow_id, "series_count": count}
    _fetch_meta_path(layout, namespace).write_text(json.dumps(meta), encoding="utf-8")
    return meta


def _resolve_series_count(layout: BuildRoot, namespace: str, parquet_path: Path) -> int:
    meta_path = _fetch_meta_path(layout, namespace)
    if meta_path.is_file():
        fetch_meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return int(fetch_meta["series_count"])
    import pyarrow.parquet as pq

    return int(pq.read_metadata(parquet_path).num_rows)


def _index_one(
    layout: BuildRoot,
    agency: AgencyId,
    flow_id: str,
    *,
    embedder: object | None = None,
) -> dict:
    namespace = series_namespace(agency, flow_id)
    staging = _series_staging_dir(layout, namespace)
    staging_parquet = staging / SERIES_PARQUET
    if not staging_parquet.is_file():
        raise FileNotFoundError(f"Missing staging parquet: {staging_parquet}")

    structure = load_structure_marker(layout.catalogs, agency, flow_id)
    distinct = collect_distinct_from_columnar(staging_parquet, structure.dsd_order)

    result = build_flow_catalog(
        series_parquet=staging_parquet,
        namespace=namespace,
        agency=agency,
        flow_id=flow_id,
        structure=structure,
        catalogs_dir=layout.catalogs,
        staging_dir=layout.staging / "catalog_partial",
        embedder=embedder,  # type: ignore[arg-type]
        distinct=distinct,
    )
    index_meta = {
        "namespace": result.namespace,
        "agency": agency.value,
        "flow_id": flow_id,
        "series_count": result.series_count,
        "catalog_kind": result.catalog_kind,
    }
    (staging / "index_meta.json").write_text(json.dumps(index_meta), encoding="utf-8")
    return index_meta


def run_series_index(
    layout: BuildRoot,
    agency: AgencyId,
    flow_id: str,
) -> dict:
    _set_memory_limit(INDEX_RLIMIT_GB)
    try:
        import torch

        torch.set_num_threads(2)
    except ImportError:
        pass
    return _index_one(layout, agency, flow_id)


def run_series_index_batch(
    layout: BuildRoot,
    agency: AgencyId,
    flow_ids: list[str],
) -> list[dict]:
    """Build columnar stores for many flows in one process, sharing one embedder."""
    _set_memory_limit(INDEX_RLIMIT_GB)
    try:
        import torch

        torch.set_num_threads(2)
    except ImportError:
        pass

    from parsimony.embedder import SentenceTransformerEmbedder

    embedder = SentenceTransformerEmbedder()
    results: list[dict] = []
    for flow_id in flow_ids:
        try:
            meta = _index_one(layout, agency, flow_id, embedder=embedder)
            results.append({"ok": True, "flow_id": flow_id, **meta})
        except Exception as exc:  # noqa: BLE001 - per-flow isolation inside batch
            results.append({"ok": False, "flow_id": flow_id, "error": f"{type(exc).__name__}: {exc}"})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--agency", type=AgencyId, required=True)
    parser.add_argument("--flow", help="Single flow id (series-fetch / series-index).")
    parser.add_argument(
        "--flows-file",
        help="File with newline-separated flow ids (series-index-batch).",
    )
    parser.add_argument(
        "--mode",
        choices=["series-fetch", "series-index", "series-index-batch"],
        required=True,
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    layout = BuildRoot.create(args.root)

    if args.mode == "series-index-batch":
        if not args.flows_file:
            print(json.dumps({"ok": False, "error": "series-index-batch requires --flows-file"}))
            sys.exit(1)
        flow_ids = [ln.strip() for ln in Path(args.flows_file).read_text(encoding="utf-8").splitlines() if ln.strip()]
        try:
            results = run_series_index_batch(layout, args.agency, flow_ids)
        except MemoryError as exc:
            print(json.dumps({"ok": False, "error": f"MemoryError: {exc}"}))
            sys.exit(2)
        except Exception as exc:  # noqa: BLE001 - report batch-fatal error as JSON
            print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
            sys.exit(1)
        print(json.dumps({"ok": True, "batch": True, "results": results}))
        sys.exit(0)

    if not args.flow:
        print(json.dumps({"ok": False, "error": f"{args.mode} requires --flow"}))
        sys.exit(1)
    try:
        if args.mode == "series-fetch":
            payload = run_series_fetch(layout, args.agency, args.flow)
        else:
            payload = run_series_index(layout, args.agency, args.flow)
    except MemoryError as exc:
        print(json.dumps({"ok": False, "error": f"MemoryError: {exc}"}))
        sys.exit(2)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        sys.exit(1)

    print(json.dumps({"ok": True, **payload}))
    sys.exit(0)


if __name__ == "__main__":
    main()
