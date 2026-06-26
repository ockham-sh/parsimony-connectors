"""Build and publish the two-tier BLS catalogs.

Builds a per-survey series catalog (``bls_series_<survey>``) for each headline
survey, collects each one's dimension manifest, then builds the tier-1 surveys
catalog (``bls_surveys``) with those manifests attached. Snapshots are written
under namespace subdirectories of ``--save-root`` / ``--push-root``.

    uv run python packages/bls/scripts/build_catalog.py \
        --save-root /tmp/parsimony-catalogs/bls --push-root hf://parsimony-dev/bls

    # one survey only
    uv run python packages/bls/scripts/build_catalog.py --survey CU --save-root /tmp/bls
"""

from __future__ import annotations

import argparse
import logging
import os

from parsimony_bls.catalog_build import build_series_catalog, build_surveys_catalog
from parsimony_bls.catalog_policy import manifest_from_series_entries
from parsimony_bls.surveys import HEADLINE_SURVEYS, normalize_survey

logger = logging.getLogger(__name__)
_BUILDER = "packages/bls/scripts/build_catalog.py"


def _save_all(catalog, *, save_root: str | None, push_root: str | None) -> None:
    for root in (save_root, push_root):
        if root:
            catalog.save(f"{root.rstrip('/')}/{catalog.name}", builder=_BUILDER)


def build(
    surveys: list[str], *, save_root: str | None, push_root: str | None, max_rows: int
) -> None:
    api_key = os.environ.get("BLS_API_KEY", "")
    manifests: dict[str, list[dict[str, object]]] = {}

    for sv in surveys:
        try:
            catalog = build_series_catalog(sv, max_rows=max_rows)
        except Exception as exc:  # noqa: BLE001 -- per-survey resilience for the batch
            logger.warning("skip series catalog for %s: %s", sv, exc)
            continue
        manifests[sv] = manifest_from_series_entries(catalog.entities)
        logger.info("built %s with %d series", catalog.name, len(catalog.entities))
        _save_all(catalog, save_root=save_root, push_root=push_root)

    surveys_catalog = build_surveys_catalog(api_key=api_key, manifests=manifests)
    logger.info(
        "built %s with %d surveys (%d manifests)",
        surveys_catalog.name,
        len(surveys_catalog.entities),
        len(manifests),
    )
    _save_all(surveys_catalog, save_root=save_root, push_root=push_root)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--survey",
        action="append",
        help="Survey abbreviation to build (repeatable). Default: the headline allowlist.",
    )
    parser.add_argument("--save-root", help="Local root; snapshots go to <root>/<namespace>.")
    parser.add_argument("--push-root", help="Remote root, e.g. hf://parsimony-dev/bls.")
    parser.add_argument("--max-rows", type=int, default=0, help="Cap series per survey (0 = all).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    surveys = (
        [normalize_survey(s) for s in args.survey]
        if args.survey
        else sorted(HEADLINE_SURVEYS)
    )
    build(surveys, save_root=args.save_root, push_root=args.push_root, max_rows=args.max_rows)


if __name__ == "__main__":
    main()
