"""CLI argument parsing → frozen ``RunConfig``."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from parsimony_sdmx.providers.agencies import AGENCY_IDS

EPILOG = """\
Examples:
  parsimony-sdmx -a ECB --list-datasets              # print available datasets
  parsimony-sdmx -a ESTAT --catalog                   # write only datasets.parquet
  parsimony-sdmx -a ECB -d YC                         # one dataset
  parsimony-sdmx -a ESTAT --all --dry-run             # preview full-agency run
"""


@dataclass(frozen=True, slots=True)
class RunConfig:
    agency_id: str
    dataset_id: str | None
    all_datasets: bool
    list_datasets: bool
    catalog_only: bool
    dry_run: bool
    force: bool
    output_base: Path
    verbose: bool
    dataset_timeout_s: float | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parsimony-sdmx",
        description=(
            "Flat SDMX catalog builder. Writes per-agency parquet files: "
            "outputs/{AGENCY}/datasets.parquet and "
            "outputs/{AGENCY}/series/{DATASET}.parquet."
        ),
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-a",
        "--agency",
        required=True,
        choices=sorted(AGENCY_IDS),
        help="Agency to fetch from.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-d", "--dataset", help="Single dataset ID to fetch.")
    mode.add_argument(
        "--all", action="store_true", help="Fetch every dataset for the agency."
    )
    mode.add_argument(
        "--list-datasets",
        action="store_true",
        help="Print available datasets and exit (no fetch).",
    )
    mode.add_argument(
        "--catalog",
        action="store_true",
        help=(
            "Write only outputs/{AGENCY}/datasets.parquet (one row per "
            "dataset, columns dataset_id/agency_id/title). No series fetched."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the exact parquet paths that would be written and exit. "
            "Implies no network fetch beyond dataset enumeration."
        ),
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help=(
            "Rebuild datasets whose series parquet already exists. Without "
            "--force, an existing outputs/{AGENCY}/series/{DATASET}.parquet "
            "is treated as a completed run and skipped (resume is "
            "filesystem-backed)."
        ),
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="outputs",
        help="Directory under which per-agency outputs are written (default: outputs/).",
    )
    parser.add_argument(
        "--dataset-timeout",
        type=float,
        default=900.0,
        help=(
            "Per-dataset wall-clock ceiling in seconds (default: 900 = 15 min). "
            "A dataset exceeding this is killed and reported as FAILED(timeout); "
            "the run continues. Pass 0 to disable (unbounded)."
        ),
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable DEBUG-level logging."
    )
    return parser


def parse_args(argv: list[str] | None = None) -> RunConfig:
    parser = build_parser()
    ns = parser.parse_args(argv)
    timeout = None if ns.dataset_timeout <= 0 else float(ns.dataset_timeout)
    return RunConfig(
        agency_id=ns.agency,
        dataset_id=ns.dataset,
        all_datasets=bool(ns.all),
        list_datasets=bool(ns.list_datasets),
        catalog_only=bool(ns.catalog),
        dry_run=bool(ns.dry_run),
        force=bool(ns.force),
        output_base=Path(ns.output_dir).resolve(),
        verbose=bool(ns.verbose),
        dataset_timeout_s=timeout,
    )
