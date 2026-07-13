"""Live integration tests for parsimony-snb (Swiss National Bank).

Hits the real public SNB data portal (``data.snb.ch``). SNB is **keyless**, so
these tests need no env vars and run without secrets (no ``assert_no_secret_leak``).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/snb -m integration

**Bounded crawls only.** A full ``enumerate_snb`` discovers ~1,149 cubes from the
sitemap and fans out a metadata request per cube. The live enumerate test
monkeypatches the ``_list_cubes`` seam to a 2-cube slice (one publication, one
warehouse) and a request counter asserts the bound held. ``snb_search`` is covered
against a locally-built fixture catalog, never the published snapshot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result
from parsimony.transport import HttpClient
from parsimony_test_support import assert_provenance_shape

from parsimony_snb import SNB_ENUMERATE_OUTPUT, snb_fetch
from parsimony_snb.connectors import enumerate as enum_mod
from parsimony_snb.connectors.enumerate import _list_cubes, enumerate_snb
from parsimony_snb.search import snb_search

pytestmark = pytest.mark.integration

# A real publication cube (bond yields) + a real warehouse cube (outstanding
# derivatives), used to bound the live enumerate fan-out.
_BOUNDED_CUBES: list[tuple[str, str, str]] = [
    ("rendoblim", "publication", "ziredev"),
    ("BSTA@SNB.AUR_U.ODF", "warehouse", "BSTA"),
]


def test_snb_fetch_rendoblim_live() -> None:
    """rendoblim (Swiss Confederation bond yields) — a stable monthly publication cube."""
    result = snb_fetch(cube_id="rendoblim", from_date="2024")

    assert_provenance_shape(result, expected_source="snb_fetch", required_param_keys=["cube_id"])
    df = result.raw
    assert not df.empty, "SNB fetch of rendoblim returned an empty DataFrame"
    assert set(df["cube_id"]) == {"rendoblim"}
    # Real title from getCubeInfo, not the cube_id fallback.
    assert (df["title"] != "rendoblim").any(), "title never resolved beyond the cube_id fallback"
    assert "Value" in df.columns and df["Value"].dtype.kind == "f"
    assert df["Value"].notna().any(), "no real observation values"
    vals = df["Value"].dropna()
    assert ((vals > -2.0) & (vals < 6.0)).all(), f"yields out of plausible range: {vals.tolist()[:5]}"
    assert df["D0"].astype(str).str.len().gt(0).all(), "dimension codes lost to coercion"
    assert df["date"].dtype.kind == "M" and df["date"].notna().any()


def test_snb_fetch_devkum_multidim_live() -> None:
    """devkum (FX rates) is a two-dimension cube — exercises the multi-dim parse."""
    df = snb_fetch(cube_id="devkum", from_date="2024").raw
    assert not df.empty
    assert {"D0", "D1", "Value"} <= set(df.columns)
    assert df["D1"].nunique() > 1, "expected multiple currencies in the FX cube"
    assert df["Value"].notna().any()


def test_snb_fetch_warehouse_live() -> None:
    """A data-warehouse cube fetches via /api/warehouse/cube/{@→.}/... (the Q2 gap).

    The old connector excluded all warehouse cubes; this proves one is fetchable
    end-to-end through ``snb_fetch``'s id-shape routing.
    """
    df = snb_fetch(cube_id="BSTA@SNB.AUR_U.ODF", from_date="2020").raw
    assert not df.empty, "warehouse fetch returned empty"
    assert set(df["cube_id"]) == {"BSTA@SNB.AUR_U.ODF"}
    # The warehouse CSV carries named dimension columns + a numeric Value.
    assert "Value" in df.columns and df["Value"].notna().any()
    dim_cols = [c for c in df.columns if c not in {"date", "Value", "cube_id", "title"}]
    assert dim_cols, "no warehouse dimension columns parsed"
    assert df[dim_cols[0]].astype(str).str.len().gt(0).all(), "warehouse dim codes lost"


def test_list_cubes_sitemap_live() -> None:
    """The live sitemap yields the full universe (~237 publication + ~912 warehouse).

    A floor check (not an equality) so a new SNB cube doesn't fail the test — but a
    big shrink (a broken parse / sitemap move) trips it.
    """
    cubes = _list_cubes()
    pub = [c for c in cubes if c[1] == "publication"]
    wh = [c for c in cubes if c[1] == "warehouse"]
    assert len(pub) >= 220, f"too few publication cubes: {len(pub)}"
    assert len(wh) >= 850, f"too few warehouse cubes: {len(wh)} (warehouse discovery broke?)"
    # No warehouse-style id should leak into the publication set and vice-versa.
    assert all("@" not in cid for cid, _, _ in pub)
    assert all("@" in cid for cid, _, _ in wh)


def test_enumerate_snb_bounded_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl TWO real cubes (one publication, one warehouse) to verify the live
    metadata + dimensions shape without the full ~1,149-cube fan-out."""

    def _bounded() -> list[tuple[str, str, str]]:
        return _BOUNDED_CUBES

    monkeypatch.setattr(enum_mod, "_list_cubes", _bounded)

    real_request = HttpClient.request
    calls: list[str] = []

    def _counting_request(self: Any, method: str, path: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(path)
        return real_request(self, method, path, *args, **kwargs)

    monkeypatch.setattr(HttpClient, "request", _counting_request)

    df = enumerate_snb().raw

    # 2 cubes: getCubeInfo ×2 + dimensions ×1 (warehouse has no dims call) ≈ 3, never ~2,300.
    assert len(calls) < 12, f"bounded crawl fired {len(calls)} requests — bound did not hold"
    assert list(df.columns) == [c.name for c in SNB_ENUMERATE_OUTPUT.columns]
    assert not df.empty
    assert set(df["source"]) == {"snb_data_portal", "snb_warehouse"}

    # Real content in declared columns.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank series title"
    assert df["description"].astype(str).str.len().gt(0).any()
    pub_codes = set(df[df["source"] == "snb_data_portal"]["code"])
    assert any(c.startswith("rendoblim#") for c in pub_codes)
    wh = df[df["source"] == "snb_warehouse"]
    assert (wh["code"] == "BSTA@SNB.AUR_U.ODF#").any()

    entities = list(Result(raw=df, output_spec=SNB_ENUMERATE_OUTPUT).entities.values())
    assert len(entities) == len(df)
    assert entities[0].namespace == "snb"


def test_snb_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``snb_search`` over a small, locally-built catalog (network-free,
    never a cold full build)."""
    cols = [c.name for c in SNB_ENUMERATE_OUTPUT.columns]

    def _row(code: str, title: str, description: str, source: str, **over: str) -> dict[str, str]:
        base = dict.fromkeys(cols, "")
        base.update(code=code, title=title, description=description, source=source)
        base.update(over)
        return base

    rows = [
        _row(
            "rendoblim#10J",
            "10 years — Yields on Swiss Confederation bond issues",
            "Interest rates and exchange rates. CHF Swiss Confederation bond issues / 10 years.",
            "snb_data_portal",
            cube_id="rendoblim",
            series_key="10J",
        ),
        _row(
            "devkum#M0.USD1",
            "USD 1 — Foreign exchange rates",
            "Foreign exchange rates. Monthly average / America / USD 1.",
            "snb_data_portal",
            cube_id="devkum",
            series_key="M0.USD1",
        ),
        _row(
            "BSTA@SNB.AUR_U.ODF#",
            "Outstanding derivative financial instruments",
            "Annual banking statistics. Outstanding derivative financial instruments.",
            "snb_warehouse",
            cube_id="BSTA@SNB.AUR_U.ODF",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = list(Result(raw=df, output_spec=SNB_ENUMERATE_OUTPUT).entities.values())
    catalog = Catalog("snb", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "snb_catalog"
    catalog.save(out_dir)

    result = snb_search(query="Swiss Confederation bond yields", limit=5, catalog_url=str(out_dir))
    assert_provenance_shape(result, expected_source="snb_search", required_param_keys=["query"])
    sdf = result.raw
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty
    assert sdf.iloc[0]["code"] == "rendoblim#10J"

    # Ranking discriminates: the warehouse derivatives cube is the top hit for its query.
    wh = snb_search(query="outstanding derivative financial instruments", limit=5, catalog_url=str(out_dir))
    assert wh.raw.iloc[0]["code"] == "BSTA@SNB.AUR_U.ODF#"
