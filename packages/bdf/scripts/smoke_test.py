"""Retrieval smoke test for the published BdF catalog.

Loads the local file-backed catalog and runs ~12 representative queries
(EN + FR, macro topics) to gauge top-k plausibility.
"""

from __future__ import annotations

import asyncio
import sys

from parsimony.cache import catalogs_dir
from parsimony.catalog import Catalog

CATALOG_URL = f"file://{catalogs_dir('bdf')}/bdf"

QUERIES: list[str] = [
    "France inflation rate",
    "indice des prix à la consommation",
    "EUR USD exchange rate",
    "taux de change euro dollar",
    "French government debt",
    "dette publique française",
    "interest rates eurozone",
    "taux d'intérêt zone euro",
    "balance of payments France",
    "balance des paiements france",
    "household savings rate",
    "ECB policy rate",
    "Livret A rate",
    "monetary aggregates eurozone",
    "non-financial corporations debt",
]


async def _main() -> int:
    cat = await Catalog.from_url(CATALOG_URL)
    print(f"Loaded catalog from {CATALOG_URL}", flush=True)

    for q in QUERIES:
        hits = await cat.search(q, limit=5)
        print(f"\n=== {q} ===", flush=True)
        if not hits:
            print("  <no hits>", flush=True)
            continue
        for h in hits:
            title = (h.title or "")[:90]
            desc = (h.description or "")[:80].replace("\n", " ")
            print(f"  [{h.similarity:.3f}] {h.code}", flush=True)
            print(f"           title: {title}", flush=True)
            if desc:
                print(f"           desc:  {desc}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
