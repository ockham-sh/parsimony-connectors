#!/usr/bin/env bash
# Parsimony v1 acceptance gate.
# Run from parsimony-connectors with path-source siblings synced.
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
from parsimony.errors import RateLimitError
from parsimony_riksbank import riksbank_fetch
from parsimony_treasury import treasury_fetch
def main():
    try:
        r = riksbank_fetch("SEKEURPMI")
        assert len(r.raw) > 0
        return
    except RateLimitError:
        pass
    # Bare Fiscal Data endpoint — not a catalog compound code.
    r = treasury_fetch("v2/accounting/od/debt_to_penny")
    assert len(r.raw) > 0
main()
PY
then ok "bare fetch"; else bad "bare fetch"; fi

note "Gate 2 — catalog search actionable error without [catalog] (simulated)"
if uv run python - <<'PY'
from unittest.mock import patch
from parsimony.errors import ConnectorError
from parsimony.catalog import Catalog
from parsimony_riksbank.search import riksbank_search
def main():
    with patch.object(Catalog, "load", side_effect=ImportError("No module named 'faiss'")):
        try:
            riksbank_search("SEKEURPMI", catalog_url="file:///tmp/none")
        except ConnectorError as exc:
            assert "parsimony[catalog]" in str(exc)
            return
        raise AssertionError("expected ConnectorError")
main()
PY
then ok "missing catalog stack error"; else bad "missing catalog stack error"; fi

CATALOG_ROOT="${PARSIMONY_ACCEPTANCE_CATALOG_ROOT:-file:///tmp/parsimony-catalogs-v1/treasury}"
note "Gate 3 — search→fetch with local v1 catalog ($CATALOG_ROOT)"
if uv run python - <<PY
import os
from parsimony.errors import RateLimitError
os.environ["PARSIMONY_TREASURY_CATALOG_URL"] = "$CATALOG_ROOT"
from parsimony_treasury import load
def main():
    c = load()
    hits = c["treasury_search"](query="GDP", limit=3)
    assert len(hits.raw) > 0
    row = hits.raw.iloc[0]
    # Dispatch from METADATA — never paste the compound code into fetch.
    source = str(row["source"])
    endpoint = str(row["endpoint"])
    try:
        if source == "treasury_rates":
            rows = c["treasury_rates_fetch"](feed=endpoint)
        else:
            assert source == "fiscal_data", source
            rows = c["treasury_fetch"](endpoint=endpoint)
    except RateLimitError:
        return  # upstream quota — search leg already proved
    assert len(rows.raw) > 0
main()
PY
then ok "treasury search→fetch"; else bad "treasury search→fetch"; fi

note "Gate 4 — SDMX discovery chain (local catalogs if present)"
SDMX_ROOT="${PARSIMONY_ACCEPTANCE_SDMX_ROOT:-file:///tmp/parsimony-catalogs-v1/sdmx}"
if uv run python - <<PY
import os
from pathlib import Path
root = "$SDMX_ROOT".removeprefix("file://")
if not (Path(root) / "sdmx_datasets_ecb" / "meta.json").exists():
    raise SystemExit(0)  # skip — catalogs not built in this env
os.environ["PARSIMONY_SDMX_CATALOG_URL"] = "$SDMX_ROOT"
from parsimony_sdmx import load
def main():
    c = load()
    ds = c["sdmx_datasets_search"](agency="ECB", query="yield curve", limit=3)
    assert len(ds.raw) > 0
    assert {"agency", "dataset_id", "score", "search_detail"}.issubset(ds.raw.columns)
    row = ds.raw.iloc[0]
    agency = str(row["agency"])
    dataset_id = str(row["dataset_id"])
    series_ns = Path(root) / f"sdmx_series_{agency.lower()}_{dataset_id.lower()}"
    if not (series_ns / "meta.json").exists():
        return  # datasets leg already proved; series catalog may be unpublished
    series = c["sdmx_series_search"](agency=agency, dataset_id=dataset_id, query="rate", limit=3)
    assert len(series.raw) > 0
    assert "key" in series.raw.columns
main()
PY
then ok "sdmx datasets + series search"; else bad "sdmx datasets + series search"; fi

note "Gate 5 — keyed connector names env var (fred)"
if uv run python - <<'PY'
from parsimony.errors import UnauthorizedError
from parsimony_fred import fred_fetch
def main():
    try:
        fred_fetch("GDP")
    except UnauthorizedError as exc:
        assert exc.env_var == "FRED_API_KEY"
main()
PY
then ok "fred env var"; else bad "fred env var"; fi

note "Gate 8 — parsimony list --strict"
if uv run parsimony list --strict; then ok "parsimony list --strict"; else bad "parsimony list --strict"; fi

echo ""
echo "Acceptance gate: $PASS passed, $FAIL failed"
test "$FAIL" -eq 0
