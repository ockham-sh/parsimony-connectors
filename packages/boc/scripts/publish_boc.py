"""Publish parsimony-boc catalog snapshots.

Self-contained driver — delegates to ``parsimony.publish.publish_provider``
with the default ONNX embedder. Output stages to
``parsimony.cache.catalogs_dir('bde')`` — the standard XDG cache
(``~/.cache/parsimony/catalogs/boc/`` on Linux). Override with
``PARSIMONY_CACHE_DIR`` if you need a different cache root (HF runners,
alternate disks).

Run with ``uv run --extra publish python scripts/publish_boc.py`` from
the package root. After publishing, push to HF::

    hf upload ockham/boc "$(parsimony cache info | grep catalogs)/boc/"
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

from parsimony import OnnxEmbedder
from parsimony.cache import catalogs_dir
from parsimony.publish import publish_provider

TARGET_ROOT = catalogs_dir("boc")


async def _main() -> int:
    print(f"=== publish boc start {time.strftime('%Y-%m-%dT%H:%M:%S%z')} ===", flush=True)
    print(f"target: {TARGET_ROOT}", flush=True)
    t0 = time.time()
    report = await publish_provider(
        "bde",
        target=f"file://{TARGET_ROOT}/{{namespace}}",
        embedder=OnnxEmbedder(),
    )
    dt = time.time() - t0
    print(f"=== wall clock: {dt:.1f}s ({dt / 60:.1f} min) ===", flush=True)
    print(f"published: {len(report.published)}", flush=True)
    print(f"skipped:   {len(report.skipped)}", flush=True)
    print(f"failed:    {len(report.failed)}", flush=True)
    for ns, err in report.failed[:10]:
        print(f"  FAIL {ns}: {err[:200]}", flush=True)
    return 0 if not report.failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
