"""Build the Riksbank catalog snapshot."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from parsimony_riksbank.catalog_build import build_riksbank_catalog

_RIKSBANK_API_KEY_ENV = "RIKSBANK_API_KEY"
logger = logging.getLogger(__name__)


async def build(*, save: str | None, push: str | None, api_key: str | None) -> None:
    key = (api_key or os.environ.get(_RIKSBANK_API_KEY_ENV, "")).strip() or None
    catalog = await build_riksbank_catalog(api_key=key)
    logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
    if save is not None:
        await catalog.save(save, builder="packages/riksbank/scripts/build_catalog.py")
    if push is not None:
        await catalog.save(push, builder="packages/riksbank/scripts/build_catalog.py")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", help="Local directory to write a catalog snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/riksbank.")
    parser.add_argument(
        "--api-key",
        help=f"Optional Riksbank subscription key (fallback: {_RIKSBANK_API_KEY_ENV}).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(build(save=args.save, push=args.push, api_key=args.api_key))


if __name__ == "__main__":
    main()
