"""Publish every ECB catalog snapshot.

Self-contained driver — discovers the ECB dataflow list on the wire, then
delegates to ``parsimony.publish.publish_provider`` with the pipelined
fetch/embed path and the default ONNX embedder (MiniLM-L6 after the
2026-04-23 default change).

Log messages come through at INFO so you see one ``publishing sdmx/...``
line per namespace — otherwise the run looks silent for minutes on the
monster flows (BSI, CBD2, MFI).

Output stages to ``parsimony.cache.catalogs_dir('sdmx')`` — the standard
XDG cache (``~/.cache/parsimony/catalogs/sdmx/`` on Linux). Override with
``PARSIMONY_CACHE_DIR`` if you need a different cache root.

Run with ``uv run --extra publish python scripts/publish_ecb.py`` from
the package root.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    stream=sys.stdout,
)
# sdmx1's HTTP client emits 3 INFO lines per request (URL, full request
# headers, "Not found in cache") — tens of thousands of noisy lines on a
# multi-hour publish. Pin to WARNING so only real problems show up.
logging.getLogger("sdmx.client").setLevel(logging.WARNING)
logging.getLogger("sdmx.reader").setLevel(logging.WARNING)

from parsimony import FragmentEmbeddingCache, OnnxEmbedder
from parsimony.cache import catalogs_dir
from parsimony.publish import publish_provider

from parsimony_sdmx._isolation import list_datasets
from parsimony_sdmx.connectors.enumerate_series import series_namespace

TARGET_ROOT = catalogs_dir("sdmx")


async def _main() -> int:
    print(f"target: {TARGET_ROOT}", flush=True)
    print("discovering ECB dataflows...", flush=True)
    t0 = time.time()
    datasets = await asyncio.to_thread(list_datasets, "ECB")
    only = [series_namespace("ECB", d.dataset_id) for d in datasets]
    print(f"  {len(only)} ECB namespaces in {time.time() - t0:.1f}s", flush=True)

    print(f"=== publish start {time.strftime('%Y-%m-%dT%H:%M:%S%z')} ===", flush=True)
    emb = OnnxEmbedder()  # default is sentence-transformers/all-MiniLM-L6-v2
    cache = FragmentEmbeddingCache(emb)

    t0 = time.time()
    report = await publish_provider(
        "sdmx",
        target=f"file://{TARGET_ROOT}/{{namespace}}",
        only=only,
        embedder=emb,
        fragment_cache=cache,
    )
    dt = time.time() - t0

    cache.persist()

    print("", flush=True)
    print(f"=== wall clock: {dt:.1f}s ({dt / 60:.1f} min) ===", flush=True)
    print(f"published: {len(report.published)}", flush=True)
    print(f"skipped:   {len(report.skipped)}", flush=True)
    print(f"failed:    {len(report.failed)}", flush=True)
    print(f"fragment cache: {cache.stats()}", flush=True)
    for ns, err in report.failed[:10]:
        print(f"  FAIL {ns}: {err[:160]}", flush=True)
    return 0 if not report.failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
