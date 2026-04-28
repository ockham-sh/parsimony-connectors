"""End-to-end smoke test: invoke the MCP search tools as an agent would.

Skips the actual MCP stdio JSON-RPC layer (Claude Desktop / Claude Code
handle that); calls the connector directly the same way the MCP server
bridge does after JSON-RPC unmarshalling. If this script returns
sensible top-1 hits, the MCP integration is wire-ready.

Usage::

    PARSIMONY_SDMX_CATALOG_ROOT=file:///home/espinet/ockham/catalogs/sdmx/repo \
      python scripts/test_mcp_smoke.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

from parsimony_sdmx.connectors.search import (
    PARSIMONY_SDMX_CATALOG_ROOT_ENV,
    SeriesSearchParams,
    sdmx_series_search,
)

# (query, flow_id, expected_substring_in_top1_title)
SMOKE_QUERIES = [
    ("Spain monthly HICP all-items annual rate", "ECB/HICP", "spain"),
    ("Germany monthly HICP all-items annual rate", "ECB/HICP", "germany"),
    ("Japanese yen Euro daily spot exchange rate", "ECB/EXR", "japanese yen"),
    ("Swiss franc Euro daily spot exchange rate", "ECB/EXR", "swiss franc"),
    ("US dollar Euro monthly spot exchange rate", "ECB/EXR", "us dollar"),
]


async def _run() -> int:
    if not os.environ.get(PARSIMONY_SDMX_CATALOG_ROOT_ENV):
        sys.stderr.write(
            f"Set {PARSIMONY_SDMX_CATALOG_ROOT_ENV} (e.g. "
            "file:///home/espinet/ockham/catalogs/sdmx/repo) before running.\n"
        )
        return 2

    print(f"=== MCP smoke against {os.environ[PARSIMONY_SDMX_CATALOG_ROOT_ENV]} ===")
    failures = 0
    for query, flow_id, expected in SMOKE_QUERIES:
        t0 = time.perf_counter()
        try:
            result = await sdmx_series_search(
                SeriesSearchParams(query=query, flow_id=flow_id, limit=5)
            )
        except Exception as exc:
            print(f"  [FAIL] {flow_id} :: {query}")
            print(f"         exception: {type(exc).__name__}: {exc}")
            failures += 1
            continue

        df = result.df
        elapsed = time.perf_counter() - t0
        top = df.iloc[0]
        ok = expected.lower() in str(top["title"]).lower()
        status = "OK  " if ok else "MISS"
        print(
            f"  [{status}] {flow_id} :: {query!r}\n"
            f"         top1: {top['series_key']}  ({elapsed*1000:.0f}ms)\n"
            f"         title: {str(top['title'])[:120]}"
        )
        if not ok:
            failures += 1

    print(f"=== {len(SMOKE_QUERIES) - failures}/{len(SMOKE_QUERIES)} passed ===")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(_run()))
