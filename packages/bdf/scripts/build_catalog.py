"""Build the Banque de France catalog snapshot."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from parsimony.catalog import BM25Index, Catalog, HybridIndex, VectorIndex
from parsimony.ranking import ZScoreFusion

from parsimony_bdf import enumerate_bdf

logger = logging.getLogger(__name__)


def _catalog() -> Catalog:
    return Catalog(
        "bdf",
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


def _api_key(explicit: str | None) -> str:
    value = explicit or os.environ.get("BANQUEDEFRANCE_KEY", "")
    if not value:
        raise ValueError("BANQUEDEFRANCE_KEY is required to build the BdF catalog")
    return value


async def build(*, api_key: str | None, save: str | None, push: str | None) -> Catalog:
    result = await enumerate_bdf(api_key=_api_key(api_key))
    catalog = _catalog()
    catalog.set_entries(result.data)
    await catalog.build()
    logger.info("Built %s catalog with %d entries", catalog.name, len(catalog))
    if save is not None:
        await catalog.save(save, builder="packages/bdf/scripts/build_catalog.py")
    if push is not None:
        await catalog.save(push, builder="packages/bdf/scripts/build_catalog.py")
    return catalog


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key", help="BdF API key. Defaults to BANQUEDEFRANCE_KEY.")
    parser.add_argument("--save", help="Local directory to write a catalog snapshot.")
    parser.add_argument("--push", help="Catalog URL to push, e.g. hf://parsimony-dev/bdf.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(build(api_key=args.api_key, save=args.save, push=args.push))


if __name__ == "__main__":
    main()
