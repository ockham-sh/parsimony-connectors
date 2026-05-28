#!/usr/bin/env bash
# Push a local catalog snapshot to Hugging Face using the modern `hf` CLI.
#
# Usage:
#   ./scripts/push_catalog.sh hf://parsimony-dev/riksbank /tmp/parsimony-catalogs/riksbank
#   ./scripts/push_catalog.sh hf://parsimony-dev/sdmx/sdmx_datasets /tmp/parsimony-catalogs/sdmx/sdmx_datasets
#
# Auth: HF_TOKEN env var (recommended) or `hf auth login`.
# Install CLI: uv tool install 'huggingface_hub[cli]'  (provides `hf` on PATH via uv tool run)

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

# hf://org/repo[/subpath]
REST="${CATALOG_URL#hf://}"
REPO_ID="${REST%%/*}"
SUBPATH=""
if [[ "$REST" == */* ]]; then
  SUBPATH="${REST#*/}"
fi

HF=(uv tool run hf)
if [[ -n "${HF_TOKEN:-}" ]]; then
  HF+=(--token "$HF_TOKEN")
fi

echo "Creating dataset repo (if needed): $REPO_ID"
"${HF[@]}" repos create "$REPO_ID" --repo-type dataset --exist-ok

if [[ -n "$SUBPATH" ]]; then
  echo "Uploading $LOCAL_DIR -> $REPO_ID (path_in_repo=$SUBPATH)"
  "${HF[@]}" upload "$REPO_ID" "$LOCAL_DIR" "$SUBPATH" \
    --repo-type dataset \
    --commit-message "$COMMIT_MSG"
else
  echo "Uploading $LOCAL_DIR -> $REPO_ID (repo root)"
  "${HF[@]}" upload "$REPO_ID" "$LOCAL_DIR" \
    --repo-type dataset \
    --commit-message "$COMMIT_MSG"
fi

echo "Done. Validate with:"
echo "  uv run python scripts/validate_catalog.py --catalog-url $CATALOG_URL"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -z "$SUBPATH" ]] || [[ "${PARSIMONY_UPDATE_DATASET_CARD:-}" == "1" ]]; then
  CARD_ROOT="$LOCAL_DIR"
  if [[ -n "$SUBPATH" && -n "${PARSIMONY_CATALOG_ROOT:-}" ]]; then
    CARD_ROOT="${PARSIMONY_CATALOG_ROOT}"
  fi
  echo "Updating dataset card README for $REPO_ID"
  uv run python "$REPO_ROOT/scripts/publish_catalog_dataset_card.py" \
    --repo-id "$REPO_ID" \
    --from-local "$CARD_ROOT" \
    --preserve-body \
    --commit-message "Refresh dataset card for HF Dataset Viewer"
fi
