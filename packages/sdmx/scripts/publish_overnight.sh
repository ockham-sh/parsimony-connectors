#!/usr/bin/env bash
# Overnight chain: publish ESTAT → IMF_DATA → WB_WDI into the parsimony
# kernel cache (``parsimony.cache.catalogs_dir('sdmx')`` —
# ``~/.cache/parsimony/catalogs/sdmx/`` on Linux). Override the cache
# root with ``PARSIMONY_CACHE_DIR`` for HF runners or alternate disks.
#
# Per-agency loop
# ---------------
# CPython does not return memory to the OS between flows, so a single
# python process eventually OOMs the host on long runs (ESTAT has 8 k+
# flows, with several producing 800 k-series catalogs that pin ~5 GB of
# heap). The wrapper recycles the publisher process every ``BATCH_SIZE``
# namespaces. Re-runs use ``--resume`` so finished snapshots are skipped
# (meta.json check) and the fragment cache is reloaded from the global
# parsimony cache (``parsimony cache info`` to inspect) so embeddings
# amortize across batches.
#
# Exit-code contract with publish_agency.py:
#   0  → agency complete; advance to next agency
#   75 → more flows remain in this agency; recycle and continue
#   *  → unexpected error; log it and advance (do not retry forever)
#
# Logs land under ``logs/`` next to this script (NOT /tmp — /tmp is wiped
# on WSL/system reboot, and this chain runs for hours). Per-batch stdout
# is appended to one log per agency.
#
# Run with:
#   nohup bash scripts/publish_overnight.sh > logs/overnight.log 2>&1 &

set -uo pipefail

# Run from the package root (parent of scripts/) so relative imports of
# parsimony_sdmx and the script paths below resolve consistently.
cd "$(dirname "$0")/.."

# Default to the workspace-level uv venv at parsimony-connectors/.venv
# (created by ``uv sync --extra publish`` from the workspace root).
# Override PARSIMONY_VENV to point at any python with the [publish] extra
# installed (e.g. a per-package venv on an HF runner).
PARSIMONY_VENV="${PARSIMONY_VENV:-$(cd ../.. && pwd)/.venv}"
source "$PARSIMONY_VENV/bin/activate"

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Force glibc to use a single small arena pool. Without this, glibc
# creates up to 8*ncpu per-thread arenas (asyncio + ONNX intra-op
# threads + FAISS + Parquet writes touch many threads). ``malloc_trim``
# only fully reclaims the main arena, so multi-arena state means freed
# memory is not returned to the OS — RSS stays at the high-water mark
# of the largest flow seen so far. Capping arenas at 2 makes
# ``parsimony.publish._release_memory`` actually release between flows.
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"

# Optional virtual-address-space cap (RLIMIT_AS / ulimit -v).
#
# This is a runaway sanity check, NOT physical-memory protection. vsize
# bears no useful relation to RSS: Python + ONNX + FAISS routinely
# reserve 25-35 GiB of vsize while using ~5-8 GiB resident. Setting
# this cap too tight produces spurious MemoryError on tiny allocations.
# Physical-memory protection is .wslconfig (memory=28GB) + OOM killer.
#
# Default: 0 (disabled). Set PARSIMONY_VSIZE_CAP_GIB to a generous
# number (e.g. 60) if you want a hard ceiling on truly runaway vsize.
VSIZE_CAP_GIB="${PARSIMONY_VSIZE_CAP_GIB:-0}"
if [ "$VSIZE_CAP_GIB" -gt 0 ]; then
    ulimit -v $((VSIZE_CAP_GIB * 1024 * 1024))
fi

# Tunables
BATCH_SIZE="${PARSIMONY_PUBLISH_BATCH_SIZE:-15}"     # flows per python process
MAX_BATCHES_PER_AGENCY="${PARSIMONY_MAX_BATCHES:-1000}"  # safety stop

log() {
    echo "[$(date -Iseconds)] $*"
}

log "MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX  BATCH_SIZE=$BATCH_SIZE  MAX_BATCHES=$MAX_BATCHES_PER_AGENCY"
if [ "$VSIZE_CAP_GIB" -gt 0 ]; then
    log "vsize cap: ${VSIZE_CAP_GIB} GiB (virtual address space, not physical)"
else
    log "vsize cap: disabled (PARSIMONY_VSIZE_CAP_GIB=0)"
fi

# 1. Wait for any in-flight ECB publish to finish.
log "=== overnight: waiting for in-flight publish_ecb (if any) ==="
while pgrep -f "python scripts/publish_ecb.py" >/dev/null; do
    sleep 30
done
log "=== overnight: no publish_ecb running, starting agency chain ==="

# 2. Publish each remaining agency, recycling the python process every
#    BATCH_SIZE flows until publish_agency returns 0 (all done) or hits
#    the per-agency safety cap.
for AGENCY in ESTAT IMF_DATA WB_WDI; do
    LOG="$LOG_DIR/publish_${AGENCY,,}.log"
    log "=== overnight: starting $AGENCY (batch=$BATCH_SIZE) → $LOG ==="

    BATCH=0
    while :; do
        BATCH=$((BATCH + 1))
        if [ "$BATCH" -gt "$MAX_BATCHES_PER_AGENCY" ]; then
            log "=== overnight: $AGENCY hit safety cap of $MAX_BATCHES_PER_AGENCY batches — moving on ==="
            break
        fi

        log "=== overnight: $AGENCY batch #$BATCH ==="
        # Append (>>) so all batches' output for one agency lands in one
        # log file. Each batch writes its own start/end banner.
        python scripts/publish_agency.py "$AGENCY" \
               --resume \
               --max-batch "$BATCH_SIZE" \
            >> "$LOG" 2>&1
        rc=$?

        case $rc in
            0)
                log "=== overnight: $AGENCY complete after $BATCH batch(es) ==="
                break
                ;;
            75)
                log "=== overnight: $AGENCY batch #$BATCH done; more remaining, recycling ==="
                continue
                ;;
            *)
                log "=== overnight: $AGENCY batch #$BATCH FAILED rc=$rc — see $LOG; aborting agency ==="
                break
                ;;
        esac
    done
done

log "=== overnight: chain complete ==="
