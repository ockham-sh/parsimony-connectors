#!/usr/bin/env bash
# Thin wrapper — canonical implementation lives in tooling/.
exec "$(cd "$(dirname "$0")/.." && pwd)/tooling/push_catalog.sh" "$@"
