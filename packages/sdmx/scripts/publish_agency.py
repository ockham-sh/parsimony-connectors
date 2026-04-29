"""Publish every catalog for ONE SDMX agency.

Generic over agency: ``ECB``, ``ESTAT``, ``IMF_DATA``, ``WB_WDI``.
Mirrors ``publish_ecb.py``'s wiring (OnnxEmbedder + FragmentEmbeddingCache
sharing the same on-disk fragment store across agencies for cross-run
amortization).

Usage::

    uv run --extra publish python scripts/publish_agency.py ECB
    uv run --extra publish python scripts/publish_agency.py ESTAT --resume
    uv run --extra publish python scripts/publish_agency.py ESTAT --resume --max-batch 30

Output stages to ``parsimony.cache.catalogs_dir('sdmx')`` — the standard
XDG cache (``~/.cache/parsimony/catalogs/sdmx/`` on Linux). Override with
``PARSIMONY_CACHE_DIR`` if you need a different cache root.

Exit codes
----------
* ``0``  — every namespace selected for this run was attempted; nothing
  left to do for this agency. Per-flow failures (network IncompleteRead,
  ESTAT ``$DV_*`` validation) are logged but do not flip the exit code,
  so the wrapper's restart loop is not stopped by a handful of bad flows
  among thousands of healthy ones.
* ``75`` (``EX_TEMPFAIL``) — ``--max-batch`` capped the run; more
  namespaces remain unpublished. The wrapper (``publish_overnight.sh``)
  treats this as the signal to recycle the Python process and re-run
  the same command. Recycling is the only reliable way to release RAM
  back to the OS over a long ESTAT publish (8 k flows, monotonic heap).
* non-zero (other) — uncaught exception in the publisher itself (not a
  per-flow fetch error). The wrapper logs it and aborts this agency.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    stream=sys.stdout,
)
# sdmx1's HTTP client emits 3 INFO lines per request (URL, full request
# headers, "Not found in cache"). Over an 8 k-flow ESTAT publish that's
# tens of thousands of lines of pure noise. Pin it to WARNING so only
# real wire-level problems surface in the log.
logging.getLogger("sdmx.client").setLevel(logging.WARNING)
logging.getLogger("sdmx.reader").setLevel(logging.WARNING)

from dataclasses import asdict

from parsimony import FragmentEmbeddingCache, OnnxEmbedder
from parsimony.cache import TTLDiskCache, catalogs_dir, connectors_dir
from parsimony.publish import publish_provider

from parsimony_sdmx._isolation import list_datasets
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.enumerate_series import series_namespace
from parsimony_sdmx.core.models import DatasetRecord

# 1:1 mirror of the HF dataset repo. The fragment embedding cache lives
# alongside under the same parsimony cache root and amortizes fragments
# across agencies ("Monthly", "Spain", … hit both ECB and ESTAT).
TARGET_ROOT = catalogs_dir("sdmx")

# Memoize SDMX dataflow listings across batches. Each list_datasets()
# call is ~30s and runs once per ``publish_agency.py`` invocation; the
# overnight wrapper recycles the process every BATCH_SIZE flows, so
# without a cache we re-enumerate the same 7 k+ ESTAT dataflows ~500
# times per chain (~3-4 h pure overhead). 24 h TTL is well within the
# stability window of these listings — agencies rarely add/remove flows
# day-to-day. Run ``parsimony cache clear --subdir connectors`` to
# force a fresh enumeration.
DATAFLOW_TTL_S = 24 * 3600


async def _resolve_datasets(agency_id: str) -> list[DatasetRecord]:
    """Return ``list_datasets(agency_id)`` from disk cache or live call."""
    cache = TTLDiskCache(connectors_dir("sdmx"))
    cache_key = f"datasets-{agency_id}"
    cached = cache.get(cache_key, max_age_s=DATAFLOW_TTL_S)
    if cached is not None:
        try:
            datasets = [DatasetRecord(**d) for d in cached]
            print(
                f"  using cached {agency_id} dataflows ({len(datasets)} flows, "
                f"TTL {DATAFLOW_TTL_S // 3600}h)",
                flush=True,
            )
            return datasets
        except TypeError:
            print(
                f"  cached {agency_id} dataflows have stale schema; refreshing",
                flush=True,
            )

    print(f"discovering {agency_id} dataflows...", flush=True)
    t0 = time.time()
    datasets = await asyncio.to_thread(list_datasets, agency_id)
    print(f"  discovered in {time.time() - t0:.1f}s", flush=True)
    cache.put(cache_key, [asdict(d) for d in datasets])
    return datasets


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "agency",
        choices=[a.value for a in AgencyId],
        help="SDMX agency ID.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Skip namespaces whose snapshot directory already exists "
            "(meta.json present). Use after an interrupted run to avoid "
            "re-fetching + re-embedding catalogs that completed."
        ),
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Cap this run at N namespaces, then exit with code 75 so a "
            "wrapper can recycle the Python process. Defaults to 0 "
            "(unbounded). Recommended for ESTAT (~30) — Python's heap "
            "does not return memory to the OS, so process recycling is "
            "the only reliable cap on RSS over a multi-thousand-flow "
            "publish."
        ),
    )
    args = parser.parse_args()
    agency_id = args.agency
    max_batch = max(0, args.max_batch)

    print(f"target: {TARGET_ROOT}", flush=True)
    datasets = await _resolve_datasets(agency_id)

    # ESTAT exposes "$DV_*" pseudo-dataflows (derived views) that are not
    # fetchable as series. Filter them here so they never enter ``only`` —
    # otherwise pydantic ValidationError tracebacks flood the log.
    publishable = [d for d in datasets if "$" not in d.dataset_id]
    skipped_dv = len(datasets) - len(publishable)
    only = [series_namespace(agency_id, d.dataset_id) for d in publishable]
    msg = f"  {len(only)} {agency_id} namespaces"
    if skipped_dv:
        msg += f" (skipped {skipped_dv} $DV_* derived-view flows)"
    print(msg, flush=True)

    if args.resume:
        # A snapshot is "done" iff its meta.json exists. We check meta
        # rather than just the directory because a half-written snapshot
        # may have created the dir (Catalog.save uses tmp + rename, so a
        # successful rename means meta is present).
        before = len(only)
        only = [
            ns for ns in only
            if not (TARGET_ROOT / ns / "meta.json").exists()
        ]
        skipped = before - len(only)
        print(
            f"  --resume: skipping {skipped} already-published; "
            f"{len(only)} remaining",
            flush=True,
        )
        if not only:
            print("=== nothing to do; everything already published ===", flush=True)
            return 0

    # Cap this run at max_batch flows (if set) and remember whether more
    # work remains so we can return EX_TEMPFAIL (75) at the end.
    more_remaining = False
    if max_batch and len(only) > max_batch:
        more_remaining = True
        only = only[:max_batch]
        print(
            f"  --max-batch {max_batch}: processing first {len(only)} this run "
            "(rest will resume after process recycle)",
            flush=True,
        )

    print(f"=== publish {agency_id} start {time.strftime('%Y-%m-%dT%H:%M:%S%z')} ===", flush=True)
    emb = OnnxEmbedder()
    cache = FragmentEmbeddingCache(emb)

    t0 = time.time()
    try:
        report = await publish_provider(
            "sdmx",
            target=f"file://{TARGET_ROOT}/{{namespace}}",
            only=only,
            embedder=emb,
            fragment_cache=cache,
            # Phase-separated fetch/embed within each batch:
            #   Phase 1 — up to 2 SDMX fetches run in parallel; each
            #     stages its Result to a parquet on disk and drops the
            #     in-memory DataFrame.
            #   Phase 2 — sequential embed/index/push reads parquets one
            #     at a time. No fetch subprocess is alive while a Catalog
            #     is in flight, so the mega-flow embed peak (~25 GB on
            #     Spain-class flows) cannot collide with subprocess RAM.
            # Eurostat (~minutes per dataflow) is the dominant batch cost;
            # K=2 hides most of that latency without re-introducing the
            # fetch+embed overlap that previously OOM-killed the host.
            fetch_concurrency=2,
        )
    finally:
        # Belt-and-suspenders: ``parsimony.publish`` already persists
        # after every flow. This catches orchestrator-level failures
        # (KeyboardInterrupt, SIGTERM with raised handler, exceptions
        # outside the per-flow loop) so the in-memory cache from this
        # batch survives no matter how we exit. SIGKILL (rc=137) cannot
        # be intercepted — only the per-flow persists protect against that.
        try:
            cache.persist()
        except Exception:
            print(
                f"WARN: end-of-batch cache.persist() failed: "
                f"{traceback.format_exc()}",
                flush=True,
            )
    dt = time.time() - t0

    print("", flush=True)
    print(f"=== {agency_id} wall clock: {dt:.1f}s ({dt / 60:.1f} min) ===", flush=True)
    print(f"published: {len(report.published)}", flush=True)
    print(f"skipped:   {len(report.skipped)}", flush=True)
    print(f"failed:    {len(report.failed)}", flush=True)
    print(f"fragment cache: {cache.stats()}", flush=True)
    for ns, err in report.failed[:20]:
        print(f"  FAIL {ns}: {err[:200]}", flush=True)

    # Exit-code semantics for the wrapper (publish_overnight.sh):
    #   75 = "more flows remain after this batch — restart me"
    #    0 = "every selected namespace was attempted; nothing left"
    # Per-flow failures (network IncompleteRead, ESTAT $DV_* validation,
    # etc.) are NOT fatal — they're logged above and reflected in the
    # ``failed:`` count. Returning non-zero on per-flow failure would
    # stop the restart loop and abandon thousands of healthy flows
    # because of a handful of bad ones.
    #
    # Hard memory pressure surfaces as an uncaught ``MemoryError`` from
    # ``publish_provider`` and exits with a non-zero code; the wrapper
    # treats that as a recycle signal too.
    if more_remaining:
        return 75
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
