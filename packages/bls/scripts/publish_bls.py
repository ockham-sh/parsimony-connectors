"""Publish parsimony-bls catalog snapshots.

Self-contained driver — delegates to ``parsimony.publish.publish_provider``
with the English ONNX embedder
(``sentence-transformers/all-MiniLM-L6-v2``) and a persistent
``FragmentEmbeddingCache``.

Nine surveys (cb, cd, ch, cs, fa, fi, fw, hc, oe) opt into the
DIMENSIONAL fragment-policy path: the runtime mean-pools per-dim
fragment vectors instead of embedding ~50 M raw titles. The cache
amortizes unique fragment vectors across all rows that share an
industry / occupation / area / event label, taking the publish
wall-time from ~14 days to ~2.5 hours. See
``PLAN-bls-fragment-policy.md`` for the structural justification.

Output stages to ``parsimony.cache.catalogs_dir('bls')`` — the standard
XDG cache (``~/.cache/parsimony/catalogs/bls/`` on Linux). Override with
``PARSIMONY_CACHE_DIR`` if you need a different cache root.

Run with ``uv run --extra publish python scripts/publish_bls.py`` from
the package root. After publishing, push to HF::

    hf upload ockham/bls "$(parsimony cache info | grep catalogs)/bls/"
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger("publish_bls")

from parsimony import FragmentEmbeddingCache, OnnxEmbedder
from parsimony.cache import catalogs_dir
from parsimony.publish import publish_provider

TARGET_ROOT = catalogs_dir("bls")
STAGING_DIR = Path("~/.cache/parsimony-bls/staging").expanduser()


async def _main() -> int:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    only_env = os.environ.get("BLS_ONLY", "").strip()
    only = [n.strip() for n in only_env.split(",") if n.strip()] or None

    embedder = OnnxEmbedder(model="sentence-transformers/all-MiniLM-L6-v2")
    fragment_cache = FragmentEmbeddingCache(embedder)

    print(f"=== publish bls start {time.strftime('%Y-%m-%dT%H:%M:%S%z')} ===", flush=True)
    print(f"target: {TARGET_ROOT}", flush=True)
    if only:
        print(f"only namespaces: {only}", flush=True)
    t0 = time.time()
    report = await publish_provider(
        "bls",
        target=f"file://{TARGET_ROOT}/{{namespace}}",
        embedder=embedder,
        fragment_cache=fragment_cache,
        fetch_concurrency=8,
        staging_dir=STAGING_DIR,
        only=only,
    )
    fragment_cache.persist()
    dt = time.time() - t0
    print(f"=== wall clock: {dt:.1f}s ({dt / 60:.1f} min, {dt / 3600:.2f} hr) ===", flush=True)
    print(f"published: {len(report.published)}", flush=True)
    print(f"skipped:   {len(report.skipped)}", flush=True)
    print(f"failed:    {len(report.failed)}", flush=True)
    print(f"fragment cache stats: {fragment_cache.stats()}", flush=True)
    for ns, err in report.failed[:10]:
        print(f"  FAIL {ns}: {err[:200]}", flush=True)
    return 0 if not report.failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
