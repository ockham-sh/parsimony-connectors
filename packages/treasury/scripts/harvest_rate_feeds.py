"""Cross-validate the curated ODM rate-feed registry against the live feeds.

The Office of Debt Management feed set + per-feed maturities are a curated registry
(``parsimony_treasury.rate_feeds``, archetype D — there is no machine-readable "list all
feeds/columns" endpoint). This script is the committed reproduction the guidebook mandates
for any frozen registry: it fetches a recent year of each feed, extracts the benchmark
columns the live feed actually carries, and diffs them against the registry — flagging

  * **NEW IN LIVE** — a benchmark maturity the feed added that the registry should catalog
    (Treasury keeps adding tenors: the 4-month in 2022, the 1.5-month par point + 6-week
    bill in 2025); and
  * **IN REGISTRY, NOT IN THIS YEAR** — a maturity the registry catalogs that the chosen
    year's feed lacks. Recently-added tenors are sparse and were absent before their
    introduction, so run against a **recent** year (the default current year) to validate
    the current schema; an older year will legitimately lack them.

Caveat: OData omits null properties per-entry, so a sparse new tenor is invisible in any
single entry — the live column set must be read as the UNION across all rows (which
``parse_treasury_rates_xml`` → DataFrame gives us).

Usage::

    uv run python packages/treasury/scripts/harvest_rate_feeds.py --year 2025
    uv run python packages/treasury/scripts/harvest_rate_feeds.py --diff        # current year
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

from parsimony_treasury import _http, parsing
from parsimony_treasury.rate_feeds import RATE_FEED_NAMES, registry_columns

logger = logging.getLogger(__name__)

_LONG_FORMAT_FEEDS = {"daily_treasury_long_term_rate", "daily_treasury_real_long_term"}


def _benchmark_live_columns(feed: str, live_cols: set[str]) -> set[str]:
    """The subset of *live_cols* the registry would catalog as benchmarks for *feed*."""
    if feed in _LONG_FORMAT_FEEDS:
        # Long-format feeds: the rate lives in a single ``RATE`` column (pivoted by RATE_TYPE).
        return {"RATE"} & live_cols
    return {c for c in live_cols if c.startswith(("BC_", "TC_", "ROUND_B1_")) and not c.endswith("DISPLAY")}


def _live_columns(feed: str, year: int) -> set[str]:
    xml_text = _http.get_text(
        _http.rates_client(),
        _http.RATES_PATH,
        params={"data": feed, "field_tdr_date_value": str(year)},
        op_name=f"harvest/{feed}/{year}",
    )
    df = parsing.parse_treasury_rates_xml(xml_text)
    return set(df.columns)


def harvest(*, year: int) -> int:
    drift = 0
    for feed in sorted(RATE_FEED_NAMES):
        live = _live_columns(feed, year)
        benchmark_live = _benchmark_live_columns(feed, live)
        registry = registry_columns(feed)
        new_in_live = benchmark_live - registry
        missing = registry - benchmark_live
        status = "OK" if not (new_in_live or missing) else "DRIFT"
        logger.info("%-32s %s  (registry=%d live-benchmark=%d)", feed, status, len(registry), len(benchmark_live))
        if new_in_live:
            logger.info("    NEW IN LIVE (add to registry): %s", sorted(new_in_live))
            drift += len(new_in_live)
        if missing:
            logger.info("    IN REGISTRY, NOT IN %d (phantom or discontinued-historical): %s", year, sorted(missing))
            drift += len(missing)
    return drift


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=datetime.now(tz=UTC).year, help="Calendar year to harvest.")
    parser.add_argument("--diff", action="store_true", help="Exit non-zero if any drift is found.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    drift = harvest(year=args.year)
    if args.diff and drift:
        raise SystemExit(f"{drift} column(s) drifted between the registry and the {args.year} live feeds")


if __name__ == "__main__":
    main()
