#!/usr/bin/env bash
# Parsimony v1 acceptance gate (RELEASE_TECH_DEBT.md §4).
# Run from a clean venv with all 22 connectors installed, or from the
# connectors worktree with path-source siblings synced.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PASS=0
FAIL=0
note() { echo "==> $*"; }
ok() { echo "OK: $*"; PASS=$((PASS + 1)); }
bad() { echo "FAIL: $*"; FAIL=$((FAIL + 1)); }

note "Gate 1 — bare fetch (riksbank; treasury fallback on upstream 429)"
if uv run python - <<'PY'
import asyncio
from parsimony.errors import RateLimitError
from parsimony_riksbank import riksbank_fetch
from parsimony_treasury import treasury_fetch
async def main():
    try:
        r = await riksbank_fetch("SEKEURPMI")
        assert len(r.data) > 0
        return
    except RateLimitError:
        pass
    r = await treasury_fetch("GDP")
    assert len(r.data) > 0
asyncio.run(main())
PY
then ok "bare fetch"; else bad "bare fetch"; fi

note "Gate 2 — catalog search actionable error without [catalog] (simulated)"
if uv run python - <<'PY'
import asyncio
from unittest.mock import patch
from parsimony.errors import ConnectorError
from parsimony.catalog import Catalog
from parsimony_riksbank.search import riksbank_search
async def main():
    with patch.object(Catalog, "load", side_effect=ImportError("No module named 'faiss'")):
        try:
            await riksbank_search("code: SEKEURPMI", catalog_url="file:///tmp/none")
        except ConnectorError as exc:
            assert "parsimony-core[catalog]" in str(exc)
            return
        raise AssertionError("expected ConnectorError")
asyncio.run(main())
PY
then ok "missing catalog stack error"; else bad "missing catalog stack error"; fi

CATALOG_ROOT="${PARSIMONY_ACCEPTANCE_CATALOG_ROOT:-file:///tmp/parsimony-catalogs-v1/treasury}"
note "Gate 3 — search→fetch with local v1 catalog ($CATALOG_ROOT)"
if uv run python - <<PY
import asyncio, os
from parsimony.errors import RateLimitError
os.environ["PARSIMONY_TREASURY_CATALOG_URL"] = "$CATALOG_ROOT"
from parsimony_treasury import load
async def main():
    c = load()
    hits = await c["treasury_search"]("title: GDP", limit=3)
    assert len(hits.data) > 0
    code = hits.data.iloc[0]["code"]
    try:
        rows = await c["treasury_fetch"](code)
    except RateLimitError:
        return  # upstream quota — search leg already proved
    assert len(rows.data) > 0
asyncio.run(main())
PY
then ok "treasury search→fetch"; else bad "treasury search→fetch"; fi

note "Gate 4 — SDMX discovery chain (local catalogs if present)"
SDMX_ROOT="${PARSIMONY_ACCEPTANCE_SDMX_ROOT:-file:///tmp/parsimony-catalogs-v1/sdmx}"
if uv run python - <<PY
import asyncio, os
from pathlib import Path
root = "$SDMX_ROOT".removeprefix("file://")
if not (Path(root) / "sdmx_datasets_ecb" / "meta.json").exists():
    raise SystemExit(0)  # skip — catalogs not built in this env
os.environ["PARSIMONY_SDMX_CATALOG_URL"] = "$SDMX_ROOT"
from parsimony_sdmx import load
async def main():
    c = load()
    ds = await c["sdmx_datasets_search"](agency="ECB", query="code: ECB|YC", limit=3)
    assert len(ds.data) > 0
asyncio.run(main())
PY
then ok "sdmx datasets search"; else bad "sdmx datasets search"; fi

note "Gate 5 — keyed connector names env var (fred)"
if uv run python - <<'PY'
import asyncio
from parsimony.errors import UnauthorizedError
from parsimony_fred import fred_fetch
async def main():
    try:
        await fred_fetch("GDP")
    except UnauthorizedError as exc:
        assert exc.env_var == "FRED_API_KEY"
asyncio.run(main())
PY
then ok "fred env var"; else bad "fred env var"; fi

note "Gate 8 — parsimony list --strict"
if uv run parsimony list --strict; then ok "parsimony list --strict"; else bad "parsimony list --strict"; fi

echo ""
echo "Acceptance gate: $PASS passed, $FAIL failed"
test "$FAIL" -eq 0
