#!/usr/bin/env bash
# Build all schema-v1 catalog snapshots under /tmp/parsimony-catalogs-v1.
#
# Usage (from parsimony-connectors root):
#   ./scripts/rebuild_catalogs_v1.sh              # flat + boj (fast path)
#   ./scripts/rebuild_catalogs_v1.sh --all        # includes SDMX full release (long)
#   ./scripts/rebuild_catalogs_v1.sh --provider treasury
#
# Requires network. BDF needs BDF_API_KEY; EIA needs EIA_API_KEY; Riksbank accepts optional RIKSBANK_API_KEY.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="${PARSIMONY_CATALOG_SRC:-/tmp/parsimony-catalogs-v1}"
ALL=0
PROVIDER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all) ALL=1; shift ;;
    --provider) PROVIDER="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

build_flat() {
  local pkg="$1"
  echo "==> build $pkg"
  (cd "$ROOT/packages/$pkg" && uv run python scripts/build_catalog.py --save "$DEST/$pkg")
}

build_boj() {
  echo "==> build boj"
  uv run python "$ROOT/packages/boj/scripts/build_catalog.py" \
    --catalog all --save-root "$DEST/boj"
}

build_sdmx() {
  echo "==> build sdmx (full release — may take hours)"
  uv run python "$ROOT/packages/sdmx/scripts/build_all_catalogs.py" \
    --root "$DEST/sdmx-build" --no-resume
  rm -rf "$DEST/sdmx"
  mkdir -p "$DEST/sdmx"
  shopt -s nullglob
  for ns in \
    "$DEST/sdmx-build/catalogs"/sdmx_datasets_* \
    "$DEST/sdmx-build/catalogs"/sdmx_codelist_* \
    "$DEST/sdmx-build/catalogs"/sdmx_series_*; do
    [[ -f "$ns/meta.json" ]] || continue
    cp -a "$ns" "$DEST/sdmx/"
  done
  shopt -u nullglob
}

if [[ -n "$PROVIDER" ]]; then
  case "$PROVIDER" in
    boj) build_boj ;;
    sdmx) build_sdmx ;;
    *) build_flat "$PROVIDER" ;;
  esac
  exit 0
fi

rm -rf "$DEST"
mkdir -p "$DEST"

for p in riksbank treasury rba bde boc destatis snb bdp; do
  build_flat "$p"
done

if [[ -n "${BDF_API_KEY:-}" ]]; then
  (cd "$ROOT/packages/bdf" && uv run python scripts/build_catalog.py --save "$DEST/bdf" --api-key "$BDF_API_KEY")
else
  echo "SKIP bdf (set BDF_API_KEY to include)" >&2
fi

# eia is a registered flat catalog provider but keyed — its build reads EIA_API_KEY.
if [[ -n "${EIA_API_KEY:-}" ]]; then
  build_flat eia
else
  echo "SKIP eia (set EIA_API_KEY to include)" >&2
fi

build_boj

if [[ "$ALL" -eq 1 ]]; then
  build_sdmx
else
  echo "Skipping SDMX full release (pass --all to include)" >&2
fi

echo ""
echo "Manifest:"
uv run python "$ROOT/scripts/catalog_manifest_summary.py" --catalog-root "$DEST" --audit --skip-bdf
