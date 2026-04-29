"""Phase 0a profiler: time the hot sections of a single-flow ECB publish.

Monkeypatches ``Catalog._embed_missing``, ``Catalog._rebuild_indices``, and
``Catalog._write_parquet`` with ``time.perf_counter`` wrappers so we can
see the percentage split across fetch / embed / rebuild / write for one
medium ECB flow (default: ICP — Harmonised Index of Consumer Prices).

This answers the plan's Phase 0a question: is ``_rebuild_indices``
dominant? If yes, the add_all rebuild-once change already shipped in
Phase 1 closes most of the ECB-speed gap on its own.

Usage::

    uv run python scripts/bench_ecb_flow.py                # default: ICP
    uv run python scripts/bench_ecb_flow.py --flow STS     # or any ECB dataset_id

No HF push. Output stages to ``parsimony.cache.catalogs_dir('sdmx')``
(``~/.cache/parsimony/catalogs/sdmx/`` on Linux).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    stream=sys.stdout,
)

from parsimony import OnnxEmbedder
from parsimony.cache import catalogs_dir
from parsimony.catalog import Catalog
from parsimony.publish import publish_provider

from parsimony_sdmx.connectors.enumerate_series import series_namespace

TARGET_ROOT = catalogs_dir("sdmx")


class Timings:
    """Accumulator for section-level wall-clock in a single publish run.

    Keyed by section name; each value is total seconds spent across all
    calls. Kept global-ish (bound to this script's run) so monkeypatched
    methods can dump into the same bucket regardless of which Catalog
    instance they were called on.
    """

    def __init__(self) -> None:
        self.seconds: dict[str, float] = defaultdict(float)
        self.calls: dict[str, int] = defaultdict(int)

    def record(self, name: str, seconds: float) -> None:
        self.seconds[name] += seconds
        self.calls[name] += 1

    def print_report(self, total: float) -> None:
        print("", flush=True)
        print(f"=== Phase 0a profile (total publish wall-clock: {total:.2f}s) ===", flush=True)
        print(f"{'section':<22} {'seconds':>10} {'%total':>8} {'calls':>8}", flush=True)
        print("-" * 52, flush=True)
        for name, secs in sorted(self.seconds.items(), key=lambda kv: -kv[1]):
            pct = (secs / total) * 100.0 if total > 0 else 0.0
            n = self.calls[name]
            print(f"{name:<22} {secs:>10.3f} {pct:>7.1f}% {n:>8}", flush=True)
        tracked = sum(self.seconds.values())
        untracked = total - tracked
        untracked_pct = (untracked / total) * 100.0 if total > 0 else 0.0
        print("-" * 52, flush=True)
        print(f"{'tracked':<22} {tracked:>10.3f} {(tracked / total * 100 if total else 0):>7.1f}%", flush=True)
        print(f"{'untracked (fetch+etc)':<22} {untracked:>10.3f} {untracked_pct:>7.1f}%", flush=True)


def install_timers(timings: Timings) -> None:
    """Wrap Catalog hot sections with perf_counter bookkeeping."""

    original_embed = Catalog._embed_missing
    original_rebuild = Catalog._rebuild_indices
    original_write_parquet = Catalog._write_parquet

    async def timed_embed(self, entries):  # type: ignore[no-untyped-def]
        start = time.perf_counter()
        try:
            return await original_embed(self, entries)
        finally:
            timings.record("embed (_embed_missing)", time.perf_counter() - start)

    def timed_rebuild(self):  # type: ignore[no-untyped-def]
        start = time.perf_counter()
        try:
            return original_rebuild(self)
        finally:
            timings.record("rebuild_indices", time.perf_counter() - start)

    def timed_write_parquet(self, target, info):  # type: ignore[no-untyped-def]
        start = time.perf_counter()
        try:
            return original_write_parquet(self, target, info)
        finally:
            timings.record("write_parquet", time.perf_counter() - start)

    Catalog._embed_missing = timed_embed  # type: ignore[method-assign]
    Catalog._rebuild_indices = timed_rebuild  # type: ignore[method-assign]
    Catalog._write_parquet = timed_write_parquet  # type: ignore[method-assign]

    try:
        from parsimony.indexes import write_faiss as _write_faiss_orig

        def timed_write_faiss(*args, **kwargs):  # type: ignore[no-untyped-def]
            start = time.perf_counter()
            try:
                return _write_faiss_orig(*args, **kwargs)
            finally:
                timings.record("write_faiss", time.perf_counter() - start)

        import parsimony.catalog as catalog_module

        catalog_module.write_faiss = timed_write_faiss  # type: ignore[attr-defined]
    except Exception as exc:
        print(f"note: could not wrap write_faiss ({exc}); timing will roll into untracked", flush=True)


async def _main(flow: str) -> int:
    namespace = series_namespace("ECB", flow)
    print(f"=== Phase 0a bench: ECB/{flow} → {namespace} ===", flush=True)

    timings = Timings()
    install_timers(timings)

    emb = OnnxEmbedder()

    t0 = time.perf_counter()
    report = await publish_provider(
        "sdmx",
        target=f"file://{TARGET_ROOT}/{{namespace}}",
        only=[namespace],
        embedder=emb,
    )
    total = time.perf_counter() - t0

    timings.print_report(total)
    print("", flush=True)
    print(f"published: {len(report.published)}", flush=True)
    print(f"skipped:   {len(report.skipped)}", flush=True)
    print(f"failed:    {len(report.failed)}", flush=True)
    for ns, err in report.failed[:5]:
        print(f"  FAIL {ns}: {err[:200]}", flush=True)

    return 0 if not report.failed else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow", default="ICP", help="ECB dataset_id (default: ICP)")
    args = parser.parse_args()
    sys.exit(asyncio.run(_main(args.flow)))
