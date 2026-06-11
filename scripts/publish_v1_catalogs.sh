#!/usr/bin/env bash
# GATED: republish schema-v1 catalogs to parsimony-dev/* on Hugging Face.
# Requires HF_TOKEN (or `hf auth login`) and explicit maintainer approval.
#
# Usage (from parsimony-connectors root, after local builds under /tmp/parsimony-catalogs-v1):
#   export HF_TOKEN=...
#   ./scripts/publish_v1_catalogs.sh
#
# Flat providers push one repo root each. Multi-bundle providers (boj, sdmx) push
# namespace subdirectories via tooling/push_catalog.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PUSH="$ROOT/tooling/push_catalog.sh"
SRC="${PARSIMONY_CATALOG_SRC:-/tmp/parsimony-catalogs-v1}"

if [[ -z "${HF_TOKEN:-}" ]] && ! uv tool run hf whoami &>/dev/null; then
  echo "Set HF_TOKEN or run 'hf auth login' before publishing." >&2
  exit 1
fi

publish_flat() {
  local provider="$1"
  local dir="$SRC/$provider"
  if [[ ! -f "$dir/meta.json" ]]; then
    echo "SKIP $provider (no snapshot at $dir)" >&2
    return 0
  fi
  echo "Publishing hf://parsimony-dev/$provider <- $dir"
  "$PUSH" "hf://parsimony-dev/$provider" "$dir" "schema v1 rebuild"
}

for provider in riksbank treasury rba bde boc destatis snb bdp; do
  publish_flat "$provider"
done

if [[ -f "$SRC/bdf/meta.json" ]]; then
  publish_flat bdf
else
  echo "SKIP bdf (requires BDF_API_KEY for local build)" >&2
fi

if [[ -d "$SRC/boj/boj_databases" ]]; then
  echo "Publishing BoJ multi-bundle root hf://parsimony-dev/boj"
  for ns in "$SRC/boj"/*; do
    [[ -f "$ns/meta.json" ]] || continue
    name="$(basename "$ns")"
    "$PUSH" "hf://parsimony-dev/boj/$name" "$ns" "schema v1 rebuild"
  done
  PARSIMONY_CATALOG_ROOT="$SRC/boj" PARSIMONY_UPDATE_DATASET_CARD=1 \
    uv run python "$ROOT/scripts/publish_catalog_dataset_card.py" \
      --repo-id parsimony-dev/boj \
      --from-local "$SRC/boj" \
      --preserve-body \
      --commit-message "Refresh BoJ dataset card"
fi

if [[ -d "$SRC/sdmx" ]]; then
  echo "Publishing SDMX multi-bundle root hf://parsimony-dev/sdmx"
  for ns in "$SRC/sdmx"/*; do
    [[ -f "$ns/meta.json" ]] || continue
    name="$(basename "$ns")"
    "$PUSH" "hf://parsimony-dev/sdmx/$name" "$ns" "schema v1 rebuild"
  done
  PARSIMONY_CATALOG_ROOT="$SRC/sdmx" PARSIMONY_UPDATE_DATASET_CARD=1 \
    uv run python "$ROOT/scripts/publish_catalog_dataset_card.py" \
      --repo-id parsimony-dev/sdmx \
      --from-local "$SRC/sdmx" \
      --preserve-body \
      --commit-message "Refresh SDMX dataset card"
fi

echo ""
echo "Post-publish validation:"
echo "  PARSIMONY_RUN_REMOTE_CATALOGS=1 uv run pytest tests/test_remote_catalogs.py -m remote_catalog"
