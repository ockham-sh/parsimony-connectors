#!/usr/bin/env python3
"""Report which Wave 1 catalog bundles exist under a local pre-warm root."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "tooling") not in sys.path:
    sys.path.insert(0, str(_REPO / "tooling"))

from catalog_validate.registry import MACRO_CATALOG_PROVIDER_IDS, PROVIDER_SPECS

DEFAULT_ROOT = Path("/tmp/parsimony-catalogs")

# Wave 1 ECB series flows (see docs/catalog-manifest.md).
ECB_WAVE1_FLOWS: tuple[str, ...] = (
    "EXR",
    "ICP",
    "BSI",
    "FM",
    "IRS",
    "YC",
    "MIR",
    "BLS",
    "BOP",
    "GFS",
    "STS",
    "RPP",
    "CISS",
)


def _has_meta(path: Path) -> bool:
    return (path / "meta.json").is_file()


def _status(root: Path, rel: str) -> str:
    p = root / rel
    if _has_meta(p):
        return "present"
    if p.is_dir():
        return "partial"
    return "missing"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog-root",
        type=Path,
        default=DEFAULT_ROOT,
        help=f"Pre-warm root (default: {DEFAULT_ROOT})",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args()
    root: Path = args.catalog_root

    rows: list[dict[str, str]] = []

    for provider in sorted(MACRO_CATALOG_PROVIDER_IDS):
        rows.append(
            {
                "provider": provider,
                "bundle": provider,
                "expected": PROVIDER_SPECS[provider].default_url,
                "status": _status(root, provider),
            }
        )

    rows.append(
        {
            "provider": "boj",
            "bundle": "boj_databases",
            "expected": "hf://parsimony-dev/boj/boj_databases",
            "status": _status(root, "boj/boj_databases"),
        }
    )

    rows.append(
        {
            "provider": "sdmx",
            "bundle": "sdmx_datasets_ecb",
            "expected": "hf://parsimony-dev/sdmx/sdmx_datasets_ecb",
            "status": _status(root, "sdmx/sdmx_datasets_ecb"),
        }
    )

    for flow in ECB_WAVE1_FLOWS:
        ns = f"sdmx_series_ecb_{flow.lower()}"
        rows.append(
            {
                "provider": "sdmx",
                "bundle": ns,
                "expected": f"hf://parsimony-dev/sdmx/{ns}",
                "status": _status(root, f"sdmx/{ns}"),
            }
        )

    if args.json:
        print(json.dumps({"catalog_root": str(root), "bundles": rows}, indent=2))
        return

    print(f"Catalog root: {root}\n")
    print(f"{'Provider':<12} {'Bundle':<28} {'Status':<8}")
    print("-" * 52)
    for row in rows:
        print(f"{row['provider']:<12} {row['bundle']:<28} {row['status']:<8}")

    missing = sum(1 for r in rows if r["status"] == "missing")
    present = sum(1 for r in rows if r["status"] == "present")
    print(f"\n{present} present, {missing} missing, {len(rows) - present - missing} partial")


if __name__ == "__main__":
    main()
