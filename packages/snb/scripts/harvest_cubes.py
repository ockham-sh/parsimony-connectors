"""Re-derive the SNB cube universe from the live sitemap (the reproduction script).

The enumerator discovers cubes live from ``/sitemap`` at build time, so there is no
frozen registry to regenerate. This script is the committed, human-runnable proof of
that derivation (the ``discover_cubes.py`` the old frozen-registry comment promised but
never shipped): it fetches the sitemap, parses it, and reports the cube universe by
family / topic / group.

    # Inspect the live universe
    uv run python packages/snb/scripts/harvest_cubes.py

    # Save a snapshot of the cube-id set
    uv run python packages/snb/scripts/harvest_cubes.py --write packages/snb/scripts/cubes.snapshot.txt

    # Diff the live universe against a saved snapshot (catches added/removed cubes)
    uv run python packages/snb/scripts/harvest_cubes.py --diff packages/snb/scripts/cubes.snapshot.txt
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

from parsimony_snb import _http, parsing

logger = logging.getLogger(__name__)


def _harvest() -> list[tuple[str, str, str]]:
    http = _http.client()
    text = _http.fetch_sitemap(http)
    return parsing.parse_sitemap(text)


def _report(cubes: list[tuple[str, str, str]]) -> None:
    pub = [c for c in cubes if c[1] == "publication"]
    wh = [c for c in cubes if c[1] == "warehouse"]
    logger.info("Total cubes: %d  (publication=%d, warehouse=%d)", len(cubes), len(pub), len(wh))
    topics = Counter(group for _, kind, group in pub)
    groups = Counter(group for _, kind, group in wh)
    logger.info("Publication topics: %s", dict(sorted(topics.items(), key=lambda kv: -kv[1])))
    logger.info("Warehouse groups:   %s", dict(sorted(groups.items(), key=lambda kv: -kv[1])))


def _diff(cubes: list[tuple[str, str, str]], snapshot_path: Path) -> int:
    live = {cid for cid, _, _ in cubes}
    saved = {line.strip() for line in snapshot_path.read_text().splitlines() if line.strip()}
    added = sorted(live - saved)
    removed = sorted(saved - live)
    if not added and not removed:
        logger.info("No drift: %d cubes match the snapshot.", len(live))
        return 0
    if added:
        logger.warning("%d ADDED cubes (in sitemap, not in snapshot): %s", len(added), added[:50])
    if removed:
        logger.warning("%d REMOVED cubes (in snapshot, not in sitemap): %s", len(removed), removed[:50])
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", metavar="FILE", help="Write the sorted cube-id set to FILE.")
    parser.add_argument("--diff", metavar="FILE", help="Diff the live universe against a saved snapshot.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    cubes = _harvest()
    _report(cubes)

    if args.write:
        ids = sorted(cid for cid, _, _ in cubes)
        Path(args.write).write_text("\n".join(ids) + "\n")
        logger.info("Wrote %d cube ids to %s", len(ids), args.write)

    if args.diff:
        sys.exit(_diff(cubes, Path(args.diff)))


if __name__ == "__main__":
    main()
