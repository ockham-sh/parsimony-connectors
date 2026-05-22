"""Build the Reserve Bank of Australia catalog snapshot."""

from __future__ import annotations

import argparse
import asyncio
import logging

from parsimony.catalog import BM25Index, Catalog, HybridIndex, VectorIndex
from parsimony.ranking import ZScoreFusion

from parsimony_rba import enumerate_rba

logger = logging.getLogger(__name__)


def _catalog() -> Catalog:
    return Catalog(
        "rba",
        indexes=[
            BM25Index("code_bm25", field="code"),
            HybridIndex(
                "title_hybrid",
                field="title",
                indexes=[
                    BM25Index("title_bm25", field="title"),
                    VectorIndex("title_vector", field="title"),
                ],
                fusion=ZScoreFusion(weights={"title_bm25": 0.5, "title_vector": 0.8}),
            ),
            HybridIndex(
                "description_hybrid",
                field="description",
                indexes=[
                    BM25Index("description_bm25", field="description"),
                    VectorIndex("description_vector", field="description"),
                ],
                fusion=ZScoreFusion(weights={"description_bm25": 0.7, "description_vector": 1.0}),
            ),
        ],
        default_field="title",
    )


async def build(*, save: str | None, push: str | None) -> Catalog:
    result = await enumerate_rba()
    catalog = _catalog()
    catalog.set_entries(result.data)
    await catalog.build()
    logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
    if save is not None:
        await catalog.save(save, builder="packages/rba/scripts/build_catalog.py")
    if push is not None:
        await catalog.save(push, builder="packages/rba/scripts/build_catalog.py")
    return catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", help="Local directory to write a catalog snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/rba.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(build(save=args.save, push=args.push))


if __name__ == "__main__":
    main()
