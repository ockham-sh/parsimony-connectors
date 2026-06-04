"""Live integration tests for parsimony-snb (Swiss National Bank).

Hits the real public SNB data portal (``data.snb.ch``). SNB is **keyless**,
so these tests need no env vars and run without secrets — there is no key to
bind and no secret that could leak (no ``assert_no_secret_leak`` needed).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/snb -m integration

**Bounded crawls only.** A full ``enumerate_snb`` probes ~237 cubes × two
requests each (~474 requests). The live enumerate test monkeypatches the
module-level ``_KNOWN_CUBES`` registry down to two tiny real cubes so the
crawl fires only a handful of requests, and a request counter wrapped around
``HttpClient.request`` asserts the bound actually held. ``snb_search`` is
covered against a locally-built fixture catalog rather than the published
snapshot, so it never triggers a cold full enumerate + embed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw
from parsimony.transport import HttpClient
from parsimony_test_support import assert_provenance_shape

import parsimony_snb
from parsimony_snb import SNB_ENUMERATE_OUTPUT, enumerate_snb, snb_fetch
from parsimony_snb.search import snb_search

pytestmark = pytest.mark.integration

# Two small, stable real SNB cubes used to bound the live enumerate fan-out.
# rendoblim (bond yields, single-dim) + devkum (FX rates, two-dim) are both
# well-known monetary/FX cubes whose CSV shape exercises both the single- and
# multi-dimension parse paths.
_BOUNDED_CUBES: tuple[tuple[str, str], ...] = (
    ("rendoblim", "Yields on bond issues ‒ 2002 methodology (up to July 2025) (Month)"),
    ("devkum", "Foreign exchange rates (Month)"),
)


@pytest.mark.asyncio
async def test_snb_fetch_rendoblim_live() -> None:
    """rendoblim (Swiss Confederation bond yields) — a stable monthly cube."""
    result = await snb_fetch(cube_id="rendoblim", from_date="2024")

    assert_provenance_shape(result, expected_source="snb_fetch", required_param_keys=["cube_id"])
    df = result.data
    assert not df.empty, "SNB fetch of rendoblim returned an empty DataFrame"
    assert set(df["cube_id"]) == {"rendoblim"}
    # Real title from the dimensions endpoint, not the cube_id fallback.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert (df["title"] != "rendoblim").any(), "title never resolved beyond the cube_id fallback"
    # Real numeric observation values (the measure column), not just shape.
    assert "Value" in df.columns
    assert df["Value"].dtype.kind == "f"
    assert df["Value"].notna().any(), "no real observation values"
    assert df["Value"].nunique() > 1, "values implausibly constant"
    # Swiss bond yields sit in a sane band (roughly -2% to 6%).
    vals = df["Value"].dropna()
    assert ((vals > -2.0) & (vals < 6.0)).all(), f"yields out of plausible range: {vals.tolist()[:5]}"
    # The dimension code column stays a string (not NaN-coerced).
    assert df["D0"].astype(str).str.len().gt(0).all(), "dimension codes lost to coercion"
    # Dates parse to real datetimes (declared dtype="datetime").
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "observation dates all NaT"


@pytest.mark.asyncio
async def test_snb_fetch_devkum_multidim_live() -> None:
    """devkum (FX rates) is a two-dimension cube (D0 month-type × D1 currency)
    — exercises the multi-dimension long-format parse against the real feed."""
    result = await snb_fetch(cube_id="devkum", from_date="2024")
    df = result.data
    assert not df.empty
    # Two dimension code columns present alongside the measure.
    assert {"D0", "D1", "Value"} <= set(df.columns)
    assert df["D1"].nunique() > 1, "expected multiple currencies in the FX cube"
    assert df["Value"].notna().any(), "no real FX values"


@pytest.mark.asyncio
async def test_enumerate_snb_bounded_cubes_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl TWO real cubes to verify the live SNB CSV/dimensions shape without
    the full ~474-request fan-out. A request counter asserts the bound held."""
    monkeypatch.setattr(parsimony_snb, "_KNOWN_CUBES", _BOUNDED_CUBES)

    # Instrument the kernel HttpClient so we can assert the bound actually held:
    # 2 cubes × 2 probe requests = 4, not ~474.
    real_request = HttpClient.request
    calls: list[str] = []

    async def _counting_request(self: Any, method: str, path: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(path)
        return await real_request(self, method, path, *args, **kwargs)

    monkeypatch.setattr(HttpClient, "request", _counting_request)

    result = await enumerate_snb()
    df = result.data

    # The bound held: 2 cubes × 2 probes each = a handful, never ~474.
    assert len(calls) < 12, f"bounded crawl fired {len(calls)} requests — bound did not hold"

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in SNB_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"
    assert set(df["cube_id"]) == {"rendoblim", "devkum"}

    # Real content in declared columns — not just column names.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank series title"
    assert df["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert (df["source"] == "snb_data_portal").all(), "source metadata missing"
    # Frequency was inferred from the live CSV (both cubes are monthly).
    assert (df["frequency"] == "Monthly").all(), f"frequency not inferred live: {set(df['frequency'])}"

    # Compound codes are the cube_id#series_key scheme snb_fetch understands.
    codes = set(df["code"])
    assert any(c.startswith("rendoblim#") for c in codes)
    assert any(c.startswith("devkum#") for c in codes)

    # build_entities round-trips on the real slice (the catalog-build entry point).
    entities = SNB_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "snb"


@pytest.mark.asyncio
async def test_snb_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``snb_search`` end-to-end over a small, locally-built catalog.

    Bounded by design: a cold full ``build_snb_catalog()`` runs the expensive
    ~474-request enumerate + embeds thousands of rows. We build a 3-row catalog
    from real enumerator-shaped rows, persist it, and point ``catalog_url`` at
    it — so the search runs network-free and never triggers the expensive build.
    """
    cols = [c.name for c in SNB_ENUMERATE_OUTPUT.columns]

    def _row(code: str, title: str, description: str, **over: str) -> dict[str, str]:
        base = dict.fromkeys(cols, "")
        base.update(code=code, title=title, description=description, source="snb_data_portal")
        base.update(over)
        return base

    rows = [
        _row(
            "rendoblim#10J",
            "10 years — Yields on Swiss Confederation bond issues",
            "Yields on bond issues. CHF Swiss Confederation bond issues / 10 years.",
            cube_id="rendoblim",
            series_key="10J",
        ),
        _row(
            "devkum#M0.USD1",
            "USD 1 — Foreign exchange rates",
            "Foreign exchange rates. Monthly average / America / USD 1.",
            cube_id="devkum",
            series_key="M0.USD1",
        ),
        _row(
            "plkopr#0",
            "Consumer prices (Total)",
            "Swiss consumer price index, all items, monthly inflation.",
            cube_id="plkopr",
            series_key="0",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = entities_from_raw(df, SNB_ENUMERATE_OUTPUT)
    catalog = Catalog("snb", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    await catalog.build()
    out_dir = tmp_path / "snb_catalog"
    await catalog.save(out_dir)

    result = await snb_search(query="Swiss Confederation bond yields", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="snb_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "search exceeded the fixture catalog"
    # Real ranking: the bond entry is the top hit; scores are populated.
    assert sdf.iloc[0]["code"] == "rendoblim#10J"
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different
    # series as the top hit (not the same row regardless of query).
    fx = await snb_search(query="US dollar exchange rate", limit=5, catalog_url=str(out_dir))
    assert fx.data.iloc[0]["code"] == "devkum#M0.USD1"
