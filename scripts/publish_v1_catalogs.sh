#!/usr/bin/env bash
# GATED: republish schema-v1 catalogs to parsimony-dev/* on Hugging Face.
# Requires HF_TOKEN (or `hf auth login`) and explicit maintainer approval.
#
# Usage (from parsimony-connectors root, after local builds under /tmp/parsimony-catalogs-v1):
#   export HF_TOKEN=...
#   ./scripts/publish_v1_catalogs.sh
#
# Dry run:
#   ./scripts/publish_v1_catalogs.sh --dry-run

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${PARSIMONY_CATALOG_SRC:-/tmp/parsimony-catalogs-v1}"
EXTRA=()
if [[ "${1:-}" == "--dry-run" ]]; then
  EXTRA+=(--dry-run)
fi

exec uv run python "$ROOT/tooling/publish_v1_catalogs.py" --src "$SRC" "${EXTRA[@]}"
