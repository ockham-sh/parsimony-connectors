"""Build the Destatis catalog snapshot."""

from __future__ import annotations

import argparse
import asyncio
import logging

from parsimony_destatis.catalog_build import build_destatis_catalog

logger = logging.getLogger(__name__)


async def build(*, save: str | None, push: str | None) -> None:
    catalog = await build_destatis_catalog()
    logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
    if save is not None:
        await catalog.save(save, builder="packages/destatis/scripts/build_catalog.py")
    if push is not None:
        await catalog.save(push, builder="packages/destatis/scripts/build_catalog.py")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", help="Local directory to write a catalog snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/destatis.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(build(save=args.save, push=args.push))


if __name__ == "__main__":
    main()
