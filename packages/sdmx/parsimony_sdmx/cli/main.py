"""Thin CLI entry point: parse args, dispatch, print summary.

The parent process deliberately never imports sdmx1 — every provider-
library touch happens inside a spawned subprocess. Dataset listings go
through :mod:`parsimony_sdmx.cli.listing`; per-dataset work goes
through :mod:`parsimony_sdmx.cli.orchestrator`. ``worker.run_dataset``
is referenced as a picklable callable but its sdmx1-touching imports
are deferred to inside the function body, so that reference alone
doesn't drag sdmx1 into the parent.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from parsimony_sdmx.cli.args import RunConfig, parse_args
from parsimony_sdmx.cli.layout import datasets_parquet, series_parquet
from parsimony_sdmx.cli.listing import ListDatasetsError, list_datasets
from parsimony_sdmx.cli.orchestrator import OrchestratorConfig, run_agency
from parsimony_sdmx.cli.summary import format_summary
from parsimony_sdmx.cli.worker import run_dataset
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.core.outcomes import DatasetOutcome, OutcomeStatus
from parsimony_sdmx.io.parquet import write_datasets

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    raw_args = list(argv) if argv is not None else list(sys.argv[1:])
    cfg = parse_args(raw_args)
    _configure_logging(cfg.verbose)

    if cfg.list_datasets:
        return _cmd_list_datasets(cfg.agency_id)

    if cfg.catalog_only:
        return _cmd_catalog(cfg)

    try:
        dataset_ids, dataset_records = _resolve_dataset_set(cfg)
    except ListDatasetsError as exc:
        _log_listing_failure(cfg.agency_id, exc)
        return 1

    if cfg.dry_run:
        return _cmd_dry_run(cfg, dataset_ids)

    if cfg.force and dataset_ids:
        existing = sum(
            1
            for ds in dataset_ids
            if series_parquet(cfg.output_base, cfg.agency_id, ds).exists()
        )
        if existing:
            logger.warning(
                "--force: %d existing parquet file(s) under %s/%s/series/ "
                "will be overwritten",
                existing,
                cfg.output_base,
                cfg.agency_id,
            )

    if dataset_records is not None:
        # --all run: write the agency-level datasets.parquet before series runs.
        write_datasets(dataset_records, cfg.output_base, cfg.agency_id)
        _log_path("datasets.parquet", datasets_parquet(cfg.output_base, cfg.agency_id))

    outcomes = run_agency(
        cfg.agency_id,
        cfg.output_base,
        dataset_ids,
        run_dataset,
        OrchestratorConfig(
            force=cfg.force,
            isolate_subprocess=True,
            per_dataset_timeout_s=cfg.dataset_timeout_s,
        ),
    )
    print(format_summary(outcomes))

    return _exit_code(outcomes)


def _cmd_list_datasets(agency_id: str) -> int:
    try:
        records = list_datasets(agency_id)
    except ListDatasetsError as exc:
        _log_listing_failure(agency_id, exc)
        return 1
    for rec in records:
        print(f"{rec.dataset_id}\t{rec.title}")
    return 0


def _cmd_catalog(cfg: RunConfig) -> int:
    """Write only ``outputs/{AGENCY}/datasets.parquet`` (no series fetched)."""
    try:
        records = list_datasets(cfg.agency_id)
    except ListDatasetsError as exc:
        _log_listing_failure(cfg.agency_id, exc)
        return 1
    path = write_datasets(records, cfg.output_base, cfg.agency_id)
    _log_path("datasets.parquet", path)
    print(f"Wrote {len(records)} dataset(s) to {path}")
    return 0


def _cmd_dry_run(cfg: RunConfig, dataset_ids: Sequence[str]) -> int:
    if not dataset_ids:
        print("No datasets would be written.")
        return 0

    ds_parquet = datasets_parquet(cfg.output_base, cfg.agency_id)
    existing = {
        ds
        for ds in dataset_ids
        if series_parquet(cfg.output_base, cfg.agency_id, ds).exists()
    }
    verb = "force-rebuild" if cfg.force else "skip"
    print(
        f"Would process {len(dataset_ids)} dataset(s) for {cfg.agency_id}: "
        f"{len(dataset_ids) - len(existing)} new, {len(existing)} already "
        f"complete ({verb} with current flags). Output base: {cfg.output_base}"
    )
    print(f"  datasets.parquet: {ds_parquet}")
    preview = dataset_ids if cfg.verbose else dataset_ids[:10]
    for ds in preview:
        state = "exists" if ds in existing else "new"
        print(f"  [{state:6s}] {series_parquet(cfg.output_base, cfg.agency_id, ds)}")
    if not cfg.verbose and len(dataset_ids) > len(preview):
        print(f"  ... {len(dataset_ids) - len(preview)} more (pass -v to see all)")
    return 0


def _resolve_dataset_set(
    cfg: RunConfig,
) -> tuple[list[str], list[DatasetRecord] | None]:
    """Return ``(dataset_ids, dataset_records_or_None)``.

    ``-d`` skips the listing entirely; ``--all`` triggers the listing
    subprocess so the parent never imports sdmx1. The records are
    returned alongside the ids so ``--all`` can write the agency-level
    ``datasets.parquet`` without a second round-trip.
    """
    if cfg.dataset_id is not None:
        return [cfg.dataset_id], None
    records = list_datasets(cfg.agency_id)
    return [r.dataset_id for r in records], records


def _exit_code(outcomes: Sequence[DatasetOutcome]) -> int:
    return 1 if any(o.status == OutcomeStatus.FAILED for o in outcomes) else 0


def _log_listing_failure(agency_id: str, exc: ListDatasetsError) -> None:
    logger.error(
        "Failed to list datasets for %s: [%s] %s",
        agency_id,
        exc.kind,
        exc,
    )
    if exc.traceback_str:
        logger.debug("Listing traceback:\n%s", exc.traceback_str)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def _log_path(label: str, path: Path) -> None:
    logging.getLogger(__name__).info("Wrote %s → %s", label, path)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
