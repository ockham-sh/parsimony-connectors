"""Benchmark per-flow SDMX series catalog build time.

Operator tooling (not part of the plugin contract). Times ``build_series_catalog``
for a representative sample of an agency's selected flows, records wall-time and
entry count per flow, and projects the cost of building the full selection.

Usage:
    uv run --package parsimony-sdmx python packages/sdmx/scripts/benchmark_series.py \
        --agency ECB --sample 8

    # Specific flows (e.g. the big/known ones):
    uv run ... benchmark_series.py --agency ESTAT --dataset-id NAMA_10_GDP --dataset-id PRC_HICP_MIDX

Sampling: with --sample N and no explicit --dataset-id, picks N flows evenly
strided across the *selected* set (select_series_records) so the sample spans
small and large flows rather than just the first N.
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import time

from parsimony_sdmx._isolation import list_datasets
from parsimony_sdmx.catalog_build import build_series_catalog
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.series_selection import select_series_records

logger = logging.getLogger(__name__)

LISTING_TIMEOUT_S = 180.0


def _strided_sample(records: list[DatasetRecord], n: int) -> list[DatasetRecord]:
    if n >= len(records) or n <= 0:
        return records
    stride = len(records) / n
    return [records[int(i * stride)] for i in range(n)]


def _bench_one(agency: AgencyId, dataset_id: str, fetch_timeout_s: float) -> dict[str, object]:
    t0 = time.monotonic()
    entries = -1
    error: str | None = None
    try:
        catalog = build_series_catalog(agency, dataset_id, fetch_timeout_s=fetch_timeout_s)
        entries = len(catalog)
    except Exception as exc:  # noqa: BLE001 — benchmark records failures, keeps going
        error = f"{type(exc).__name__}: {exc}"
    elapsed = time.monotonic() - t0
    row = {"dataset_id": dataset_id, "entries": entries, "seconds": round(elapsed, 2), "error": error}
    status = "ERR" if error else "ok"
    logger.info("[%s] %-24s entries=%-7s %6.1fs %s", status, dataset_id, entries, elapsed, error or "")
    return row


def _main(args: argparse.Namespace) -> int:
    agency = AgencyId(args.agency)

    if args.dataset_id:
        flow_ids = list(args.dataset_id)
        logger.info("Benchmarking %d explicit flow(s) for %s", len(flow_ids), agency.value)
    else:
        logger.info("Listing datasets for %s ...", agency.value)
        records = list_datasets(agency.value, LISTING_TIMEOUT_S)
        selected = select_series_records(agency, records)
        logger.info("Selection: %d / %d non-derived flows", len(selected), len(records))
        sample = _strided_sample(selected, args.sample)
        flow_ids = [r.dataset_id for r in sample]
        total_selected = len(selected)

    rows: list[dict[str, object]] = []
    bench_start = time.monotonic()
    for fid in flow_ids:
        rows.append(_bench_one(agency, fid, args.fetch_timeout_s))
    wall = time.monotonic() - bench_start

    ok = [r for r in rows if r["error"] is None]
    times = [float(s) for r in ok if isinstance((s := r["seconds"]), (int, float))]
    print("\n=== Benchmark summary ===")
    print(json.dumps(rows, indent=2))
    if times:
        mean = statistics.mean(times)
        median = statistics.median(times)
        p90 = sorted(times)[max(0, int(len(times) * 0.9) - 1)]
        print(f"\nsampled flows : {len(ok)} ok / {len(rows)} attempted")
        print(f"wall time     : {wall:.1f}s")
        print(f"per-flow mean : {mean:.1f}s   median : {median:.1f}s   p90 : {p90:.1f}s")
        if not args.dataset_id:
            est_mean = mean * total_selected
            est_p90 = p90 * total_selected
            print(f"\nfull selection: {total_selected} flows")
            print(f"  projected (mean) : {est_mean / 3600:.1f}h")
            print(f"  projected (p90)  : {est_p90 / 3600:.1f}h")
            print(f"  flows per 10h    : mean {int(36000 / mean)}  p90 {int(36000 / p90)}")
    else:
        print("No successful builds to summarize.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", required=True, help="ECB | ESTAT | IMF_DATA | WB_WDI")
    parser.add_argument("--sample", type=int, default=8, help="Strided sample size over the selected flows.")
    parser.add_argument("--dataset-id", action="append", help="Benchmark specific flow id(s) instead of sampling.")
    parser.add_argument("--fetch-timeout-s", type=float, default=900.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(_main(args))


if __name__ == "__main__":
    main()
