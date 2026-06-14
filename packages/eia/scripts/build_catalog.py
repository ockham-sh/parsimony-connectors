"""Build the EIA dataset catalog snapshot.

Operator tooling (not part of the plugin contract). Walks the live EIA v2 route
tree, builds the searchable dataset catalog, and optionally saves/pushes it.

    uv run python packages/eia/scripts/build_catalog.py --save file:///tmp/eia
    uv run python packages/eia/scripts/build_catalog.py --push hf://parsimony-dev/eia

The EIA key is read from ``--api-key`` or the ``EIA_API_KEY`` environment var.
"""

from __future__ import annotations

import argparse
import logging
import os

from parsimony_eia.catalog_build import build_eia_catalog

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key", default=os.environ.get("EIA_API_KEY"), help="EIA API key (or EIA_API_KEY).")
    parser.add_argument("--save", help="Local directory to write a catalog snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/eia.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    catalog = build_eia_catalog(api_key=args.api_key)
    logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
    if args.save is not None:
        catalog.save(args.save, builder="packages/eia/scripts/build_catalog.py")
    if args.push is not None:
        catalog.save(args.push, builder="packages/eia/scripts/build_catalog.py")


if __name__ == "__main__":
    main()
