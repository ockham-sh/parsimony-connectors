#!/usr/bin/env bash
# Push a local catalog snapshot to Hugging Face (prune target path, then upload).
#
# Usage:
#   ./tooling/push_catalog.sh hf://parsimony-dev/riksbank /tmp/parsimony-catalogs-v1/riksbank
#   ./tooling/push_catalog.sh hf://parsimony-dev/sdmx/sdmx_datasets_ecb /tmp/parsimony-catalogs-v1/sdmx/sdmx_datasets_ecb
#
# Auth: HF_TOKEN env var (recommended) or `hf auth login`.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <hf-catalog-url> <local-snapshot-dir> [commit-message]" >&2
  exit 1
fi

CATALOG_URL="$1"
LOCAL_DIR="$2"
COMMIT_MSG="${3:-catalog snapshot schema v1 rebuild}"

if [[ ! -d "$LOCAL_DIR" ]]; then
  echo "Local snapshot not found: $LOCAL_DIR" >&2
  exit 1
fi
if [[ ! -f "$LOCAL_DIR/meta.json" ]]; then
  echo "Expected meta.json at snapshot root: $LOCAL_DIR/meta.json" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PRUNE_ARGS=()
if [[ "${PARSIMONY_SKIP_PRUNE:-}" == "1" ]]; then
  PRUNE_ARGS+=(--no-prune)
fi

uv run python "$REPO_ROOT/tooling/prune_and_push_catalog.py" \
  "$CATALOG_URL" "$LOCAL_DIR" \
  --commit-message "$COMMIT_MSG" \
  "${PRUNE_ARGS[@]}"

REST="${CATALOG_URL#hf://}"
IFS='/' read -ra PARTS <<< "$REST"
REPO_ID="${PARTS[0]}/${PARTS[1]}"
SUBPATH=""
if ((${#PARTS[@]} > 2)); then
  SUBPATH="${PARTS[2]}"
  for ((i = 3; i < ${#PARTS[@]}; i++)); do
    SUBPATH+="/${PARTS[i]}"
  done
fi

echo "Done. Validate with:"
echo "  uv run python tooling/validate_catalog.py --catalog-url $CATALOG_URL"

if [[ -z "$SUBPATH" ]] || [[ "${PARSIMONY_UPDATE_DATASET_CARD:-}" == "1" ]]; then
  CARD_ROOT="$LOCAL_DIR"
  if [[ -n "$SUBPATH" && -n "${PARSIMONY_CATALOG_ROOT:-}" ]]; then
    CARD_ROOT="${PARSIMONY_CATALOG_ROOT}"
  fi
  echo "Updating dataset card README for $REPO_ID"
  uv run python "$REPO_ROOT/tooling/publish_catalog_dataset_card.py" \
    --repo-id "$REPO_ID" \
    --from-local "$CARD_ROOT" \
    --preserve-body \
    --commit-message "Refresh dataset card for HF Dataset Viewer"
fi
