"""Phase 0a synthetic bench: is _rebuild_indices the cost center Phase 1 fixes?

Avoids the flaky SDMX fetch by synthesising SeriesEntry objects with a
deterministic stub embedder. Compares two ingest paths against the same
entries:

* **add-loop**: the pre-Phase-1 semantic — ``add()`` in batches of 100,
  triggering ``_rebuild_indices`` once per batch.
* **add_all**: the Phase-1 path — one ``add_all()`` call, one rebuild.

Both paths execute the SAME code on disk (Phase 1 is already merged in
this checkout). The ``add_all`` path just avoids the chunked loop; the
``add-loop`` path simulates the old ``_ingest`` by chunking explicitly.

For each corpus size we report wall clock + a rebuild-cost estimate.
At corpus sizes > 4096, FAISS switches from Flat to HNSW (per the plan)
and per-rebuild cost should grow super-linearly — that's where the
plan's "rebuild dominates" hypothesis is falsifiable.

Usage::

    uv run python scripts/bench_rebuild_dominance.py
    uv run python scripts/bench_rebuild_dominance.py --sizes 1000,5000,20000,50000
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
import time
from pathlib import Path

# Allow running from either /catalogs/sdmx or /catalogs/
sys.path.insert(0, str(Path(__file__).resolve().parent))

from parsimony.catalog import Catalog, SeriesEntry
from parsimony.embedder import EmbedderInfo


class _StubEmbedder:
    """Deterministic hash-based embedder — microsecond per call, so the
    timings we report are dominated by index construction cost, not
    embedding cost. Lets us isolate ``_rebuild_indices`` from
    ``_embed_missing`` cleanly."""

    DIM = 32  # small but realistic-ish so FAISS structures have real shape

    @property
    def dimension(self) -> int:
        return self.DIM

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest() * 2  # 64 bytes ≥ 32 floats
            raw = [digest[i] / 255.0 for i in range(self.DIM)]
            norm = sum(x * x for x in raw) ** 0.5 or 1.0
            vectors.append([x / norm for x in raw])
        return vectors

    async def embed_query(self, query: str) -> list[float]:
        (vec,) = await self.embed_texts([query])
        return vec

    def info(self) -> EmbedderInfo:
        return EmbedderInfo(model="stub/sha256", dim=self.DIM, normalize=True, package="bench-stub")


def _make_entries(n: int, *, prefix: str = "S") -> list[SeriesEntry]:
    return [
        SeriesEntry(
            namespace="bench",
            code=f"{prefix}{i:08d}",
            title=f"synthetic series {i}: monthly spain hicp all-items index",
            description=f"synthetic descriptor {i} with tokens for bm25 cost",
            tags=[f"tag-{i % 7}"],
        )
        for i in range(n)
    ]


async def _bench_add_loop(entries: list[SeriesEntry], batch_size: int = 100) -> tuple[float, int]:
    """Simulate pre-Phase-1 semantic: chunk + call add() per batch.

    Returns (wall_seconds, rebuild_count).
    """
    cat = Catalog("bench", embedder=_StubEmbedder())
    rebuild_count = 0
    original = cat._rebuild_indices

    def counting():
        nonlocal rebuild_count
        rebuild_count += 1
        original()

    cat._rebuild_indices = counting  # type: ignore[method-assign]

    start = time.perf_counter()
    for i in range(0, len(entries), batch_size):
        await cat.add(entries[i : i + batch_size])
    wall = time.perf_counter() - start
    return wall, rebuild_count


async def _bench_add_all(entries: list[SeriesEntry]) -> tuple[float, int]:
    """Post-Phase-1: single add_all() call. Returns (wall_seconds, rebuild_count)."""
    cat = Catalog("bench", embedder=_StubEmbedder())
    rebuild_count = 0
    original = cat._rebuild_indices

    def counting():
        nonlocal rebuild_count
        rebuild_count += 1
        original()

    cat._rebuild_indices = counting  # type: ignore[method-assign]

    start = time.perf_counter()
    await cat.add_all(entries)
    wall = time.perf_counter() - start
    return wall, rebuild_count


async def _main(sizes: list[int]) -> None:
    print(f"{'size':>8} {'add-loop':>12} {'rebuilds':>9} {'add_all':>12} {'rebuilds':>9} {'speedup':>9}", flush=True)
    print("-" * 68, flush=True)
    for size in sizes:
        entries = _make_entries(size)

        # Warmup (first run absorbs pandas/faiss/bm25 imports and caches)
        if size == sizes[0]:
            _, _ = await _bench_add_all(_make_entries(100))

        loop_wall, loop_rebuilds = await _bench_add_loop(entries)
        all_wall, all_rebuilds = await _bench_add_all(entries)

        speedup = loop_wall / all_wall if all_wall > 0 else float("inf")
        print(
            f"{size:>8} {loop_wall:>10.2f}s {loop_rebuilds:>9} {all_wall:>10.2f}s {all_rebuilds:>9} {speedup:>8.1f}x",
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sizes",
        default="1000,5000,10000,30000,80000",
        help="Comma-separated corpus sizes (default covers pre/post HNSW threshold of 4096)",
    )
    args = parser.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]
    asyncio.run(_main(sizes))
