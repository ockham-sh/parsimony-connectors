"""Publish a single ECB flow with FragmentEmbeddingCache wired.

Usage::

    uv run --extra publish python scripts/publish_one.py HICP
    uv run --extra publish python scripts/publish_one.py EXR --target-root /tmp/phase2

Exists for FragmentEmbeddingCache validation: republish one flow at a
time to isolate the effect of compositional embedding without re-running
the full ECB batch.

Output stages to ``parsimony.cache.catalogs_dir('sdmx')`` by default
(``~/.cache/parsimony/catalogs/sdmx/`` on Linux). Override with
``--target-root`` for ad-hoc experiments or with ``PARSIMONY_CACHE_DIR``
to redirect the entire kernel cache root.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    stream=sys.stdout,
)

from parsimony import FragmentEmbeddingCache, OnnxEmbedder
from parsimony.cache import catalogs_dir
from parsimony.publish import publish_provider

from parsimony_sdmx.connectors.enumerate_series import series_namespace


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("flow", help="ECB flow ID (e.g. HICP, EXR, STS)")
    parser.add_argument(
        "--target-root",
        type=Path,
        default=catalogs_dir("sdmx"),
        help="Directory under which the namespace directory is written.",
    )
    parser.add_argument(
        "--no-fragment-cache",
        action="store_true",
        help="Disable compositional embedding; embed via title as today.",
    )
    args = parser.parse_args()

    namespace = series_namespace("ECB", args.flow)
    args.target_root.mkdir(parents=True, exist_ok=True)

    emb = OnnxEmbedder()
    cache: FragmentEmbeddingCache | None = None
    if not args.no_fragment_cache:
        cache = FragmentEmbeddingCache(emb)

    print(f"=== publish {namespace} start {time.strftime('%Y-%m-%dT%H:%M:%S%z')} ===", flush=True)
    print(f"target: {args.target_root}", flush=True)
    t0 = time.time()
    report = await publish_provider(
        "sdmx",
        target=f"file://{args.target_root}/{{namespace}}",
        only=[namespace],
        embedder=emb,
        fragment_cache=cache,
    )
    dt = time.time() - t0

    if cache is not None:
        cache.persist()
        print(f"fragment cache: {cache.stats()}", flush=True)

    print(f"=== wall clock: {dt:.1f}s ({dt / 60:.1f} min) ===", flush=True)
    print(f"published: {len(report.published)}", flush=True)
    print(f"skipped:   {len(report.skipped)}", flush=True)
    print(f"failed:    {len(report.failed)}", flush=True)
    for ns, err in report.failed[:10]:
        print(f"  FAIL {ns}: {err[:200]}", flush=True)
    return 0 if not report.failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
