"""Live integration tests for parsimony-riksbank.

Hits the real public Riksbank SWEA + SWESTR APIs (``api.riksbank.se``).
Both are **open / keyless** for fetch and enumeration, so the fetch /
enumerate / search tests need no env vars and run without secrets.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/riksbank -m integration

**Keyless rate limit is tight.** The Riksbank gateway returns HTTP 429 after
a small burst of unauthenticated requests, so a ``_with_retry`` helper backs
off and retries on ``RateLimitError`` to keep these tests stable without a key.

**Bounded by design.** ``enumerate_riksbank`` is naturally bounded — ``/Series``
returns the full ~117-series payload in ONE request (no per-series fan-out) —
so the live enumerate test runs it directly. ``riksbank_search`` is covered
against a locally-built fixture catalog rather than the published snapshot, so
it never triggers a cold full enumerate + embed. The catalog-BUILD path is
⚠️-flagged: it requires ``RIKSBANK_API_KEY`` (not in the workspace ``.env``)
and ``pytest.skip``s cleanly when absent.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw
from parsimony.errors import RateLimitError
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_riksbank import (
    RIKSBANK_ENUMERATE_OUTPUT,
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_swestr_fetch,
)
from parsimony_riksbank.catalog_build import build_riksbank_catalog
from parsimony_riksbank.search import riksbank_search

pytestmark = pytest.mark.integration

_T = TypeVar("_T")


async def _with_retry(fn: Callable[[], Awaitable[_T]], *, attempts: int = 4, base_delay: float = 12.0) -> _T:
    """Retry an awaitable on the keyless rate limit (HTTP 429 → RateLimitError).

    The unauthenticated Riksbank gateway throttles aggressively; without a key
    a burst hits 429 with a ~40-50s reset. We honour the provider's
    ``retry_after`` hint (capped) and back off, so the live suite stays green
    without a subscription key.
    """
    last: RateLimitError | None = None
    for i in range(attempts):
        try:
            return await fn()
        except RateLimitError as exc:
            last = exc
            wait = min(float(exc.retry_after or base_delay), 60.0) if exc.retry_after else base_delay * (i + 1)
            await asyncio.sleep(wait)
    assert last is not None
    raise last


# ---------------------------------------------------------------------------
# riksbank_fetch (live, keyless)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_riksbank_fetch_sekeur_live() -> None:
    """SEKEURPMI — EUR/SEK daily mid rate — is a stable, high-traffic series."""
    result = await _with_retry(
        lambda: riksbank_fetch(series_id="SEKEURPMI", from_date="2026-01-01", to_date="2026-02-28")
    )

    assert_provenance_shape(result, expected_source="riksbank_fetch", required_param_keys=["series_id"])
    df = result.data
    assert not df.empty, "Riksbank fetch of SEKEURPMI returned an empty DataFrame"
    assert set(df["series_id"]) == {"SEKEURPMI"}
    # Real content, not just shape: title is real prose, values are real rates.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].nunique() > 1, "values implausibly constant"
    # EUR/SEK trades around 10-12 — sanity-check the magnitude.
    vals = df["value"].dropna()
    assert ((vals > 8.0) & (vals < 14.0)).all(), f"EUR/SEK out of plausible range: {vals.tolist()[:5]}"
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "observation dates all NaT"


# ---------------------------------------------------------------------------
# riksbank_swestr_fetch (live, keyless)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_riksbank_swestr_fetch_latest_rate_live() -> None:
    """The latest SWESTR overnight fixing carries a real numeric rate plus
    SWESTR's native trade metadata."""
    result = await _with_retry(lambda: riksbank_swestr_fetch(series="SWESTR"))

    assert_provenance_shape(result, expected_source="riksbank_swestr_fetch", required_param_keys=["series"])
    df = result.data
    assert not df.empty, "SWESTR latest returned empty"
    assert set(df["series"]) == {"SWESTR"}
    assert df["value"].notna().any(), "no real SWESTR rate"
    # SWESTR has tracked the Riksbank policy rate's neighbourhood — a small
    # positive percentage. Sanity-check the magnitude without pinning a value.
    vals = df["value"].dropna()
    assert ((vals > -2.0) & (vals < 15.0)).all(), f"SWESTR rate out of range: {vals.tolist()}"
    # Native metadata folds in as additional columns (no schema migration).
    assert "numberOfTransactions" in df.columns, "SWESTR trade metadata missing"


@pytest.mark.asyncio
async def test_riksbank_swestr_fetch_index_live() -> None:
    """The SWESTR index publishes ``value`` (an index level, not ``rate``);
    the connector normalises it onto the ``value`` column. This exercises a
    DIFFERENT URL family (/index) than the raw fixing — facet coverage."""
    result = await _with_retry(lambda: riksbank_swestr_fetch(series="SWESTRINDEX"))
    df = result.data
    assert not df.empty, "SWESTR index returned empty"
    assert set(df["series"]) == {"SWESTRINDEX"}
    # The index level is well above 100 (compounded since 2021 inception).
    vals = df["value"].dropna()
    assert vals.notna().any(), "no index value"
    assert (vals > 50.0).all(), f"index level implausible: {vals.tolist()}"


# ---------------------------------------------------------------------------
# enumerate_riksbank (live, keyless — single /Groups + /Series request each)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enumerate_riksbank_live() -> None:
    """Enumerate the live SWEA catalog (~117 series in one /Series request)
    plus the static SWESTR registry, asserting REAL content in the declared
    metadata columns — not just column names."""
    result = await _with_retry(lambda: enumerate_riksbank())
    df = result.data

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "live enumerate returned no rows"
    # ~117 SWEA + 7 SWESTR. Allow drift but assert a healthy count.
    assert len(df) > 50, f"implausibly few series enumerated: {len(df)}"

    swea = df[df["source"] == "swea"]
    swestr = df[df["source"] == "swestr"]
    assert len(swea) > 40, "too few SWEA series"
    assert len(swestr) == 7, "SWESTR registry must contribute exactly 7 rows"

    # Real content in the declared columns.
    assert swea["series_id"].astype(str).str.len().gt(0).all(), "blank series_id"
    assert swea["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert swea["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert swea["provider"].astype(str).str.len().gt(0).any(), "provider metadata empty"
    # Group breadcrumbs and frequency resolution populate for real rows.
    assert swea["group"].astype(str).str.len().gt(0).any(), "no group breadcrumbs resolved"
    assert (swea["frequency"] != "Unknown").any(), "every frequency came back Unknown"
    assert (swea["frequency_source"] == "group").any(), "no group-confident frequency"

    # The repo/policy rate is always present — spot-check a known series.
    assert (swea["series_id"] == "SECBREPOEFF").any() or (swea["series_id"] == "SEKEURPMI").any()

    # build_entities round-trips on the real frame (the catalog-build entry point).
    entities = RIKSBANK_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "riksbank"


# ---------------------------------------------------------------------------
# riksbank_search (live, over a locally-built fixture catalog — network-free)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_riksbank_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``riksbank_search`` end-to-end over a small, locally-built
    catalog. Bounded by design: a cold ``build_riksbank_catalog()`` would
    require the ⚠️ key and embed ~120 rows. We build a 3-row catalog from
    real enumerator-shaped rows, persist it, and point ``catalog_url`` at it.
    """
    cols = [c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns]

    def _row(series_id: str, title: str, description: str, source: str, **over: Any) -> dict[str, Any]:
        base: dict[str, Any] = dict.fromkeys(cols, "")
        base.update(series_id=series_id, title=title, description=description, source=source)
        base["series_closed"] = False
        base.update(over)
        return base

    rows = [
        _row(
            "SEKEURPMI",
            "EUR — euro mid rate against the Swedish krona",
            "Daily EUR/SEK mid exchange rate fixed at 16:15 CET.",
            "swea",
            frequency="Daily",
        ),
        _row(
            "SECBREPOEFF",
            "Policy rate",
            "The Riksbank policy rate — its most important key interest rate.",
            "swea",
            frequency="Daily",
        ),
        _row(
            "SWESTR",
            "SWESTR — Swedish Krona Short-Term Rate",
            "The transaction-based overnight reference rate for Swedish kronor.",
            "swestr",
            frequency="Daily",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = entities_from_raw(df, RIKSBANK_ENUMERATE_OUTPUT)
    catalog = Catalog("riksbank", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    out_dir = tmp_path / "riksbank_catalog"
    await catalog.save(out_dir)

    result = await riksbank_search(query="euro Swedish krona exchange rate", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="riksbank_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "search exceeded the fixture catalog"
    assert sdf.iloc[0]["code"] == "SEKEURPMI", "FX query did not surface the EUR series first"
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different hit.
    swestr_hit = await riksbank_search(
        query="overnight reference rate SWESTR benchmark", limit=5, catalog_url=str(out_dir)
    )
    assert swestr_hit.data.iloc[0]["code"] == "SWESTR"


# ---------------------------------------------------------------------------
# catalog-build live path (⚠️ requires RIKSBANK_API_KEY — skips cleanly)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_riksbank_catalog_live() -> None:
    """Full live catalog build. ⚠️ The keyless quota cannot sustain a full
    ~117-series enumeration, so ``build_riksbank_catalog`` requires a key and
    this test skips cleanly when ``RIKSBANK_API_KEY`` is absent (it is not in
    the workspace ``.env``)."""
    creds = require_env("RIKSBANK_API_KEY")
    catalog = await build_riksbank_catalog(api_key=creds["RIKSBANK_API_KEY"])
    assert catalog.name == "riksbank"
    # ~117 SWEA series + 7 static SWESTR rows.
    assert len(catalog) > 50, f"live build produced implausibly few entries: {len(catalog)}"
