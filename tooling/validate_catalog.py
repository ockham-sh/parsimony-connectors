#!/usr/bin/env python3
"""Validate a catalog snapshot and optional curated search probes.

Maintainer tooling only — not part of any connector plugin surface.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT / "tooling") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "tooling"))

from catalog_validate.fixtures import load_queries_file, probes_to_yaml, write_queries_file
from catalog_validate.probes import generate_probes, inspect_snapshot
from catalog_validate.registry import PROVIDER_SPECS, SDMX_QUERIES_FILE
from catalog_validate.runner import format_report, validate_catalog

logger = logging.getLogger(__name__)


def _default_url(provider: str | None) -> str | None:
    if provider is None:
        return None
    spec = PROVIDER_SPECS.get(provider)
    return spec.default_url if spec else None


def _default_queries_file(provider: str | None) -> Path | None:
    if provider == "sdmx":
        return _REPO_ROOT / SDMX_QUERIES_FILE
    if provider and provider in PROVIDER_SPECS:
        return _REPO_ROOT / PROVIDER_SPECS[provider].queries_file
    return None


def _main(args: argparse.Namespace) -> int:
    catalog_url = args.catalog_url or _default_url(args.provider)
    if catalog_url is None:
        raise SystemExit("Provide --catalog-url or --provider")

    query_set = None
    if args.queries_file:
        query_set = load_queries_file(Path(args.queries_file))

    if args.write_queries:
        from parsimony.catalog import Catalog

        catalog = Catalog.load(catalog_url)
        probes = generate_probes(catalog, catalog_url=catalog_url, sample_size=args.sample_size, seed=args.seed)
        payload = probes_to_yaml(catalog_url=catalog_url, probes=probes)
        out = Path(args.write_queries)
        write_queries_file(out, payload)
        logger.info("Wrote %d draft probes to %s", len(probes), out)
        print(json.dumps(inspect_snapshot(catalog, catalog_url=catalog_url), indent=2))
        return 0

    report = validate_catalog(
        catalog_url,
        query_set,
        allow_missing=args.allow_missing_remote,
        catalog_root=args.catalog_root,
    )
    print(format_report(report))

    if args.inspect:
        from parsimony.catalog import Catalog

        catalog = Catalog.load(catalog_url)
        print(json.dumps(inspect_snapshot(catalog, catalog_url=catalog_url), indent=2))

    if report.skipped:
        return 0
    return 0 if report.ok else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-url", help="Snapshot URL (file:// or hf://).")
    parser.add_argument("--provider", choices=sorted(PROVIDER_SPECS) + ["sdmx"], help="Use default URL for provider.")
    parser.add_argument("--queries-file", help="Curated probes YAML; default from --provider when set.")
    parser.add_argument("--write-queries", metavar="PATH", help="Generate draft probes YAML and exit.")
    parser.add_argument("--sample-size", type=int, default=5, help="Entries to sample when generating probes.")
    parser.add_argument("--seed", type=int, default=0, help="RNG seed for probe generation.")
    parser.add_argument("--catalog-root", help="Override catalog_root in queries YAML (local SDMX bundles).")
    parser.add_argument("--allow-missing-remote", action="store_true", help="Exit 0 when catalog cannot be loaded.")
    parser.add_argument("--inspect", action="store_true", help="Print JSON inspection report after validation.")
    args = parser.parse_args()
    if args.queries_file is None:
        default_q = _default_queries_file(args.provider)
        if default_q and default_q.exists():
            args.queries_file = str(default_q)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(_main(args))


if __name__ == "__main__":
    main()
