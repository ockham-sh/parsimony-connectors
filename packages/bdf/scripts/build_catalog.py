"""Build the Banque de France catalog snapshot."""

from __future__ import annotations

import argparse
import logging
import os

from parsimony_bdf.catalog_build import build_bdf_catalog

_BDF_API_KEY_ENV = "BDF_API_KEY"
logger = logging.getLogger(__name__)


def build(*, save: str | None, push: str | None, api_key: str | None) -> None:
    key = (api_key or os.environ.get(_BDF_API_KEY_ENV, "")).strip()
    catalog = build_bdf_catalog(api_key=key or None)
    logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
    if save is not None:
        catalog.save(save, builder="packages/bdf/scripts/build_catalog.py")
    if push is not None:
        catalog.save(push, builder="packages/bdf/scripts/build_catalog.py")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", help="Local directory to write a catalog snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/bdf.")
    parser.add_argument("--api-key", help=f"BdF API key (fallback: {_BDF_API_KEY_ENV}).")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build(save=args.save, push=args.push, api_key=args.api_key)


if __name__ == "__main__":
    main()
