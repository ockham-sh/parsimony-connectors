"""Retrieval smoke test for the published BLS catalog.

Runs 20 natural-language queries against the file-backed catalog and
asserts each query returns at least three plausible hits (top-3 contains
at least one expected keyword in title / description).

Designed to be flake-tolerant: checks the candidate set, not the exact
ranking, and matches keywords case-insensitively.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Sequence

from parsimony.cache import catalogs_dir
from parsimony.catalog import Catalog

# Reads from the same XDG cache the publisher writes to. Override the
# whole cache root with PARSIMONY_CACHE_DIR if you keep snapshots elsewhere.
CATALOG_ROOT = f"file://{catalogs_dir('bls')}"

# (query, namespace, expected keywords — at least one must appear
# case-insensitively somewhere in the top-3 title or description).
_QUERIES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    # ---- bls_overview: pick the right survey ------------------------
    ("consumer price index urban", "bls_overview", ("CPI", "Consumer Price")),
    ("producer price index industry", "bls_overview", ("Producer Price", "PPI")),
    ("occupational employment wages", "bls_overview", ("OEWS", "Occupational")),
    ("unemployment rate state", "bls_overview", ("Unemployment", "LAUS", "Local Area")),
    ("workplace fatality census", "bls_overview", ("Fatal", "CFOI")),
    ("job openings hires separations", "bls_overview", ("JOLTS", "Job Openings")),
    ("import export price", "bls_overview", ("International Price", "Import")),
    ("productivity unit labor cost", "bls_overview", ("Productivity",)),
    # ---- bls_series_oe: occupational queries ------------------------
    ("software developer median wage", "bls_series_oe", ("Software", "Develop")),
    ("registered nurses employment", "bls_series_oe", ("Registered Nurse", "Nurse")),
    ("chief executives Texas", "bls_series_oe", ("Chief Executive", "Executive")),
    # ---- bls_series_cu: CPI items -----------------------------------
    ("food at home", "bls_series_cu", ("Food", "food")),
    ("gasoline all urban consumers", "bls_series_cu", ("Gasoline", "Energy")),
    ("rent of primary residence", "bls_series_cu", ("Rent", "Shelter")),
    # ---- bls_series_la: state unemployment --------------------------
    ("California unemployment rate", "bls_series_la", ("California", "Unemploy")),
    ("New York labor force", "bls_series_la", ("New York", "Labor Force")),
    # ---- bls_series_ce: industry employment -------------------------
    ("manufacturing employment monthly", "bls_series_ce", ("Manufacturing",)),
    ("construction hours and earnings", "bls_series_ce", ("Construction",)),
    # ---- bls_series_cb: fatal injuries ------------------------------
    ("transportation incident fatalities", "bls_series_cb", ("Transport",)),
    ("falls slips trips", "bls_series_cb", ("Fall", "Slip", "Trip")),
)


def _hit_text(h: object) -> str:
    title = getattr(h, "title", "") or ""
    desc = getattr(h, "description", "") or ""
    return f"{title}\n{desc}".lower()


def _matches_any(hit: object, keywords: Sequence[str]) -> bool:
    text = _hit_text(hit)
    return any(k.lower() in text for k in keywords)


async def _smoke_one_namespace(
    namespace: str,
    queries: list[tuple[str, tuple[str, ...]]],
) -> tuple[int, int]:
    url = f"{CATALOG_ROOT}/{namespace}"
    cat = await Catalog.from_url(url)
    print(f"\n--- {namespace} ({len(queries)} queries) ---", flush=True)
    passed = 0
    for query, expected in queries:
        hits = await cat.search(query, limit=5)
        top3 = hits[:3]
        ok = bool(top3) and any(_matches_any(h, expected) for h in top3)
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        print(f"  [{status}] {query}", flush=True)
        for h in top3:
            title = (getattr(h, "title", "") or "")[:90]
            print(f"    [{h.similarity:.3f}] {h.code}  {title}", flush=True)
        if not ok:
            print(f"    expected one of: {expected}", flush=True)
    return passed, len(queries)


async def _main() -> int:
    by_ns: dict[str, list[tuple[str, tuple[str, ...]]]] = {}
    for query, namespace, expected in _QUERIES:
        by_ns.setdefault(namespace, []).append((query, expected))

    total_passed = 0
    total = 0
    for namespace, queries in by_ns.items():
        passed, n = await _smoke_one_namespace(namespace, queries)
        total_passed += passed
        total += n

    print(f"\n=== smoke summary: {total_passed}/{total} passed ===", flush=True)
    # Plan acceptance: 18+ of 20 (90%+) sensible top-3 candidates.
    return 0 if total_passed >= int(total * 0.9) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
