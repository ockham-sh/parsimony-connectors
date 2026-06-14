"""Live integration tests for parsimony-boc (Bank of Canada).

Hits the real public Valet JSON API (``www.bankofcanada.ca/valet``). BoC is
**keyless**, so these tests need no env vars and run without secrets — there is
no key to bind and no secret that could leak (no ``assert_no_secret_leak``
needed).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/boc -m integration

**Bounded crawls only.** A full ``enumerate_boc`` fans out one
``/groups/{name}/json`` request per group (~2,400 requests, ~1 min). The live
enumerate test monkeypatches the ``_list_groups`` seam (in
``parsimony_boc.connectors.enumerate``) down to two tiny groups so the crawl
fires only a handful of requests, and a request
counter wrapped around ``HttpClient.request`` asserts the bound actually held.
``boc_search`` is covered against a locally-built fixture catalog rather than
the published snapshot, so it never triggers a cold full enumerate + embed.
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

from parsimony_boc import BOC_ENUMERATE_OUTPUT, boc_fetch, enumerate_boc
from parsimony_boc.search import boc_search

pytestmark = pytest.mark.integration


# Two small, stable real BoC groups used to bound the live enumerate fan-out.
# FX_RATES_DAILY (~27 members) + FX_RATES_MONTHLY are the canonical FX panels.
def _bounded_groups(client: Any) -> dict[str, dict[str, Any]]:
    """Return ONLY two tiny real groups — bounds the per-group fan-out."""
    return {
        "FX_RATES_DAILY": {
            "label": "Daily exchange rates",
            "description": "Daily average exchange rates - published once each business day by 16:30 ET.",
        },
        "FX_RATES_MONTHLY": {
            "label": "Monthly average exchange rates",
            "description": "Monthly average exchange rates.",
        },
    }


def test_boc_fetch_usd_cad_fx_live() -> None:
    """FXUSDCAD — USD/CAD daily close — is one of BoC's oldest stable series."""
    result = boc_fetch(series_name="FXUSDCAD", start_date="2024-01-01", end_date="2024-03-31")

    assert_provenance_shape(result, expected_source="boc_fetch", required_param_keys=["series_name"])
    df = result.data
    assert not df.empty, "BoC fetch of FXUSDCAD returned an empty DataFrame"
    assert set(df["series_name"]) == {"FXUSDCAD"}
    # Real content, not just shape.
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].nunique() > 1, "values implausibly constant"
    # USD/CAD trades in a tight, well-known band — sanity-check magnitude.
    vals = df["value"].dropna()
    assert ((vals > 1.0) & (vals < 2.0)).all(), f"FX rate out of plausible range: {vals.tolist()[:5]}"
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "observation dates all NaT"


def test_boc_fetch_multi_series_single_request_live() -> None:
    """Two real series fetched in one comma-joined request (multi-entity)."""
    result = boc_fetch(
        series_name="FXUSDCAD,FXEURCAD",
        start_date="2024-01-01",
        end_date="2024-03-31",
    )
    df = result.data
    assert set(df["series_name"]) == {"FXUSDCAD", "FXEURCAD"}
    for serie in ("FXUSDCAD", "FXEURCAD"):
        sub = df[df["series_name"] == serie]
        assert sub["value"].notna().any(), f"{serie} has no real values"


def test_boc_fetch_group_panel_live() -> None:
    """``group:`` syntax fetches a whole real panel in one request."""
    result = boc_fetch(series_name="group:FX_RATES_DAILY", start_date="2024-03-01", end_date="2024-03-31")
    df = result.data
    assert not df.empty
    # The daily-FX group holds many currency pairs; expect well over one series.
    assert df["series_name"].nunique() > 5, "group panel returned implausibly few series"
    assert df["value"].notna().any()


def test_enumerate_boc_bounded_groups_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Crawl TWO real tiny groups to verify the live Valet shape without the
    full ~2,400-request fan-out. A request counter asserts the bound held."""
    monkeypatch.setattr("parsimony_boc.connectors.enumerate._list_groups", _bounded_groups)

    # Instrument the kernel HttpClient so we can assert the bound actually held:
    # 1 series-list request + 2 per-group membership requests = a handful, not
    # thousands. (The groups list is short-circuited by the monkeypatch.)
    real_request = HttpClient.request
    calls: list[str] = []

    def _counting_request(self: Any, method: str, path: str, *args: Any, **kwargs: Any) -> Any:
        calls.append(path)
        return real_request(self, method, path, *args, **kwargs)

    monkeypatch.setattr(HttpClient, "request", _counting_request)

    result = enumerate_boc()
    df = result.data

    # The bound held: 1 series list + 2 group-membership fetches. Generous
    # slack, but it must be a handful — never ~2,400.
    assert len(calls) < 10, f"bounded crawl fired {len(calls)} requests — bound did not hold"

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in BOC_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"
    assert set(df["entity_type"]) >= {"series", "group"}

    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) >= 1, "no series rows from the live crawl"
    # Real content in declared columns — not just column names.
    assert series_rows["title"].astype(str).str.len().gt(0).all(), "blank series title"
    assert series_rows["description"].astype(str).str.len().gt(0).any(), "no real description prose"
    assert (df["source"] == "valet").all(), "source metadata missing"

    # The two bounded groups appear as their own discoverable rows.
    group_rows = df[df["entity_type"] == "group"]
    assert set(group_rows["series_name"]) == {"group:FX_RATES_DAILY", "group:FX_RATES_MONTHLY"}

    # At least one FX series resolved its group membership from the live fan-out.
    fx_members = series_rows[series_rows["group"] == "FX_RATES_DAILY"]
    assert len(fx_members) >= 1, "no series resolved FX_RATES_DAILY membership from the live fan-out"

    # build_entities round-trips on the real slice (the catalog-build entry point).
    entities = BOC_ENUMERATE_OUTPUT.build_entities(df)
    assert len(entities) == len(df)
    assert entities[0].namespace == "boc"


def test_boc_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``boc_search`` end-to-end over a small, locally-built catalog.

    Bounded by design: a cold full ``build_boc_catalog()`` runs the expensive
    ~2,400-request enumerate + embeds ~18K rows. We build a 3-row catalog from
    real enumerator-shaped rows, persist it, and point ``catalog_url`` at it —
    so the search runs network-free and never triggers the expensive build.
    """
    cols = [c.name for c in BOC_ENUMERATE_OUTPUT.columns]

    def _row(series_name: str, title: str, description: str, **over: str) -> dict[str, str]:
        base = dict.fromkeys(cols, "")
        base.update(
            series_name=series_name,
            title=title,
            description=description,
            source="valet",
            entity_type="series",
        )
        base.update(over)
        return base

    rows = [
        _row(
            "FXUSDCAD",
            "USD/CAD daily exchange rate",
            "Daily average exchange rate: US dollars expressed in Canadian dollars.",
            group="FX_RATES_DAILY",
            group_label="Daily exchange rates",
        ),
        _row(
            "V39079",
            "Government of Canada benchmark bond yields - 10 year",
            "GoC 10-year benchmark government bond yield.",
        ),
        _row(
            "V41690973",
            "Consumer Price Index, all-items",
            "CPI all-items, Canada, monthly inflation index.",
        ),
    ]
    df = pd.DataFrame(rows, columns=cols)
    entries = entities_from_raw(df, BOC_ENUMERATE_OUTPUT)
    catalog = Catalog("boc", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "boc_catalog"
    catalog.save(out_dir)

    result = boc_search(query="US dollar Canadian dollar exchange rate", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="boc_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "search exceeded the fixture catalog"
    # Real ranking: the FX entry is the top hit; scores are populated.
    assert sdf.iloc[0]["code"] == "FXUSDCAD"
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different
    # series as the top hit (not the same row regardless of query).
    inflation = boc_search(query="consumer price index inflation", limit=5, catalog_url=str(out_dir))
    assert inflation.data.iloc[0]["code"] == "V41690973"
