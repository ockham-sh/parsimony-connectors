"""Benchmark per-flow SDMX structure fetch time."""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import time

from parsimony_sdmx._isolation import list_datasets
from parsimony_sdmx.catalog_build import build_structure_for_flow
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import DatasetRecord

logger = logging.getLogger(__name__)

LISTING_TIMEOUT_S = 180.0


def _strided_sample(records: list[DatasetRecord], n: int) -> list[DatasetRecord]:
    if n >= len(records) or n <= 0:
        return records
    step = max(1, len(records) // n)
    return [records[i] for i in range(0, len(records), step)][:n]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", type=AgencyId, required=True)
    parser.add_argument("--dataset-id", action="append")
    parser.add_argument("--sample", type=int, default=5)
    parser.add_argument("--fetch-timeout-s", type=float, default=120.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    records = [r for r in list_datasets(args.agency.value, LISTING_TIMEOUT_S) if "$" not in r.dataset_id]
    if args.dataset_id:
        wanted = {d.upper() for d in args.dataset_id}
        selected = [r for r in records if r.dataset_id.upper() in wanted]
    else:
        selected = _strided_sample(records, args.sample)

    timings: list[dict[str, float | str | int]] = []
    for record in selected:
        start = time.monotonic()
        structure = build_structure_for_flow(args.agency, record.dataset_id, fetch_timeout_s=args.fetch_timeout_s)
        elapsed = time.monotonic() - start
        timings.append(
            {
                "dataset_id": record.dataset_id,
                "seconds": round(elapsed, 2),
                "codelists": len(structure.codelists),
                "dimensions": len(structure.dimensions),
            }
        )
        logger.info(
            "STRUCT %s/%s %.1fs (%d codelists)",
            args.agency.value,
            record.dataset_id,
            elapsed,
            len(structure.codelists),
        )

    seconds = [float(t["seconds"]) for t in timings]
    summary = {
        "agency": args.agency.value,
        "flows": len(timings),
        "total_s": round(sum(seconds), 1),
        "median_s": round(statistics.median(seconds), 2) if seconds else 0,
        "timings": timings,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
