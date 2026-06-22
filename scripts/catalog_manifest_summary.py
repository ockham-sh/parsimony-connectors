#!/usr/bin/env python3
"""Report catalog bundles under the canonical pre-warm root."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "tooling") not in sys.path:
    sys.path.insert(0, str(_REPO / "tooling"))

from catalog_validate.registry import MACRO_CATALOG_PROVIDER_IDS, PROVIDER_SPECS  # noqa: E402
from catalog_validate.release_surface import (  # noqa: E402
    CANONICAL_CATALOG_ROOT,
    audit_local_root,
    classify_bundle_name,
    unreferenced_files_in_bundle,
)

SDMX_DATASET_AGENCIES = ("ecb", "estat", "imf_data", "wb_wdi")


def _count_prefix(root: Path, prefix: str) -> int:
    if not root.is_dir():
        return 0
    return sum(1 for p in root.iterdir() if p.is_dir() and p.name.startswith(prefix))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=CANONICAL_CATALOG_ROOT,
        help=f"Pre-warm root (default: {CANONICAL_CATALOG_ROOT})",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--audit", action="store_true", help="Run full release-surface audit.")
    parser.add_argument("--skip-bdf", action="store_true", help="Do not require bdf in audit.")
    args = parser.parse_args()
    root: Path = args.catalog_root

    rows: list[dict[str, str | int]] = []

    for provider in sorted(MACRO_CATALOG_PROVIDER_IDS):
        if provider == "boj":
            continue
        spec = PROVIDER_SPECS[provider]
        rel = provider
        status = "present" if (root / rel / "meta.json").is_file() else "missing"
        rows.append({"provider": provider, "bundle": provider, "expected": spec.default_url, "status": status})

    boj_root = root / "boj"
    boj_series = _count_prefix(boj_root, "boj_series_") if boj_root.is_dir() else 0
    rows.append(
        {
            "provider": "boj",
            "bundle": "boj_databases",
            "expected": "hf://parsimony-dev/boj/boj_databases",
            "status": "present" if (boj_root / "boj_databases" / "meta.json").is_file() else "missing",
        }
    )
    rows.append(
        {
            "provider": "boj",
            "bundle": f"boj_series_* ({boj_series} bundles)",
            "expected": "hf://parsimony-dev/boj/boj_series_<db>",
            "status": "present" if boj_series else "missing",
        }
    )

    sdmx_root = root / "sdmx"
    for agency in SDMX_DATASET_AGENCIES:
        bundle = f"sdmx_datasets_{agency}"
        rows.append(
            {
                "provider": "sdmx",
                "bundle": bundle,
                "expected": f"hf://parsimony-dev/sdmx/{bundle}",
                "status": "present" if (sdmx_root / bundle / "meta.json").is_file() else "missing",
            }
        )

    if sdmx_root.is_dir():
        counts = Counter(classify_bundle_name(p.name) for p in sdmx_root.iterdir() if p.is_dir())
        for kind, label in (
            ("sdmx_codelist", "sdmx_codelist_*"),
            ("sdmx_series", "sdmx_series_*"),
            ("excess", "sdmx_structure_* (build-only)"),
        ):
            rows.append(
                {
                    "provider": "sdmx",
                    "bundle": label,
                    "expected": "local build root only" if kind == "excess" else f"hf://parsimony-dev/sdmx/{label}",
                    "status": str(counts.get(kind, 0)),
                }
            )
        root_excess = []
        for name in ("meta.json", "series.parquet", "entries.parquet"):
            if (sdmx_root / name).is_file():
                root_excess.append(name)
        if root_excess:
            rows.append(
                {
                    "provider": "sdmx",
                    "bundle": "root-level excess",
                    "expected": "none",
                    "status": ", ".join(root_excess),
                }
            )

    payload: dict = {"catalog_root": str(root), "bundles": rows}
    if args.audit:
        report = audit_local_root(root, require_bdf=not args.skip_bdf)
        stale: list[dict[str, str | int]] = []
        for bundle in report.bundles:
            bundle_path = root / bundle.local_rel
            extras = unreferenced_files_in_bundle(bundle_path)
            if extras:
                stale.append({"bundle": bundle.name, "unreferenced_files": len(extras)})
        payload["audit"] = {
            "ok": report.ok,
            "publishable_bundles": len(report.bundles),
            "excess_paths": report.excess,
            "missing_required": report.missing_required,
            "bundles_with_unreferenced_files": stale,
        }

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    print(f"Catalog root: {root}\n")
    print(f"{'Provider':<12} {'Bundle':<32} {'Status':<12}")
    print("-" * 58)
    for row in rows:
        print(f"{row['provider']!s:<12} {row['bundle']!s:<32} {row['status']!s:<12}")

    if args.audit and "audit" in payload:
        audit = payload["audit"]
        print(f"\nAudit ok: {audit['ok']}")
        if audit["missing_required"]:
            print("Missing:", ", ".join(audit["missing_required"]))
        if audit["excess_paths"]:
            print(f"Excess paths ({len(audit['excess_paths'])}):", ", ".join(audit["excess_paths"][:5]), "...")
        if audit["bundles_with_unreferenced_files"]:
            print("Bundles with unreferenced files:", audit["bundles_with_unreferenced_files"])


if __name__ == "__main__":
    main()
