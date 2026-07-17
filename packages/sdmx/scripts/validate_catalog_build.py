#!/usr/bin/env python3
"""Validate an SDMX catalog release root."""

from __future__ import annotations

import argparse
import json

from parsimony_sdmx.catalog_manifest import DEFAULT_ROOT, BuildRoot
from parsimony_sdmx.release_validate import validate_release_root


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--sample-search", action="store_true")
    args = parser.parse_args()

    layout = BuildRoot.create(args.root)
    report = validate_release_root(layout, sample_search=args.sample_search)
    print(
        json.dumps(
            {
                "ok": report.ok,
                "errors": report.errors,
                "warnings": report.warnings,
                "dataset_catalog_count": len(report.dataset_catalogs),
                "series_catalog_count": len(report.series_catalogs),
            },
            indent=2,
        )
    )
    raise SystemExit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
