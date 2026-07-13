"""Live integration tests for parsimony-eia.

Hits the real ``https://api.eia.gov/v2`` endpoint. Skipped by default (root
``pyproject.toml`` sets ``-m 'not integration'``). Run with::

    uv run pytest packages/eia -m integration

Requires ``EIA_API_KEY`` (workspace contributors get it from ``ockham/.env`` via
direnv; CI sets it from secrets).

**Bounded crawls only.** ``enumerate_eia`` normally walks the whole route tree
(~272 requests → 232 datasets); the live test monkeypatches the top-route loader
to a single small category so the walk fires a handful of requests.
``eia_search`` runs over a locally-built 3-row fixture catalog rather than the
published snapshot, so it never triggers a cold full enumerate + embed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result
from parsimony_test_support import assert_no_secret_leak, assert_provenance_shape, require_env

from parsimony_eia.connectors import enumerate as enumerate_module
from parsimony_eia.connectors.enumerate import enumerate_eia
from parsimony_eia.connectors.fetch import eia_facets, eia_fetch, eia_fetch_series
from parsimony_eia.outputs import EIA_ENUMERATE_OUTPUT
from parsimony_eia.search import eia_search

pytestmark = pytest.mark.integration

# Stable, high-traffic EIA routes / series.
_SPOT_ROUTE = "petroleum/pri/spt"
_WTI_SERIES = "PET.RWTC.D"


def test_eia_fetch_petroleum_spot_prices() -> None:
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    # petroleum/pri/spt — spot prices — is a stable EIA v2 route.
    result = bound(route="petroleum/pri/spt")

    assert_provenance_shape(result, expected_source="eia_fetch", required_param_keys=["route"])
    df = result.raw
    assert not df.empty
    assert {"period", "value"}.issubset(df.columns)
    assert df["value"].notna().any(), "value column is entirely NaN"
    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


def test_eia_fetch_petroleum_spot_prices_paginates() -> None:
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    # Whole daily spot-price dataset is ~91k rows → must page well past the
    # 5,000-row API cap (the truncation bug this fix closes).
    result = bound(route=_SPOT_ROUTE, frequency="daily")

    assert_provenance_shape(result, expected_source="eia_fetch", required_param_keys=["route"])
    df = result.raw
    assert len(df) > 5000, f"pagination did not pass the 5000-row cap: got {len(df)} rows"
    assert {"period", "value"}.issubset(df.columns)
    assert df["value"].notna().any(), "value column is entirely NaN — measure facet not returned"
    assert df["period"].dtype.kind == "M", "periods did not parse to datetimes"
    assert df["period"].notna().any()
    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


def test_eia_fetch_non_value_measure_route() -> None:
    # electricity/retail-sales has NO `value` measure — valid measures are
    # revenue/sales/price/customers. Exercises the route-specific measure param
    # + the normalize-to-`value` path (the default measure would 400 here).
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    result = bound(route="electricity/retail-sales", measure="price", frequency="annual", start="2015", end="2020")

    assert_provenance_shape(result, expected_source="eia_fetch", required_param_keys=["route", "measure"])
    df = result.raw
    assert not df.empty
    assert "value" in df.columns, f"measure not normalized to value: {df.columns.tolist()}"
    assert df["value"].notna().any(), "normalized value column is entirely NaN"
    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


def test_eia_fetch_with_facet_filter_narrows() -> None:
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    # Narrow to one series via the series facet — proves facet params reach EIA.
    result = bound(
        route=_SPOT_ROUTE, frequency="daily", facets={"series": "RWTC"}, start="2024-01-01", end="2024-03-31"
    )
    df = result.raw
    assert not df.empty, "facet-filtered fetch returned nothing"
    assert (df["series"] == "RWTC").all(), "facet filter did not narrow to the requested series"
    assert df["value"].notna().any()


def test_eia_fetch_series_legacy_id_paginates() -> None:
    # The out-of-tree /v2/seriesid path: WTI crude (~10k daily rows) pages past
    # the cap and resolves a specific series straight from its legacy id.
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch_series.bind(api_key=creds["EIA_API_KEY"])

    result = bound(series_id=_WTI_SERIES)

    assert_provenance_shape(result, expected_source="eia_fetch_series", required_param_keys=["series_id"])
    df = result.raw
    assert len(df) > 5000, f"seriesid pagination did not pass the cap: {len(df)} rows"
    assert (df["series_id"] == _WTI_SERIES).all()
    assert df["value"].notna().any()
    assert df["title"].iloc[0].strip(), "series title is blank"
    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


def test_eia_facets_lists_real_values() -> None:
    creds = require_env("EIA_API_KEY")
    bound = eia_facets.bind(api_key=creds["EIA_API_KEY"])

    result = bound(route=_SPOT_ROUTE, facet="product")

    assert_provenance_shape(result, expected_source="eia_facets", required_param_keys=["route", "facet"])
    df = result.raw
    assert not df.empty, "facet value list was empty"
    assert (df["facet"] == "product").all()
    assert df["name"].str.len().gt(0).any(), "facet value names all blank"
    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


def test_enumerate_eia_lists_top_level_routes() -> None:
    creds = require_env("EIA_API_KEY")
    bound = enumerate_eia.bind(api_key=creds["EIA_API_KEY"])

    # Single request to the v2 root — cheap, no fan-out.
    result = bound()

    assert_provenance_shape(result, expected_source="enumerate_eia")
    df = result.raw
    assert not df.empty


def test_enumerate_eia_bounded_single_category_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Walk ONE real top-level category (coal, ~9 leaf datasets) by patching the
    top-route loader, so the tree fan-out fires a handful of requests rather than
    the full ~272-node crawl."""

    def _bounded_top(client: Any) -> list[str]:
        return ["coal"]

    monkeypatch.setattr(enumerate_module, "_load_top_routes", _bounded_top)
    result = enumerate_eia.bind(api_key=require_env("EIA_API_KEY")["EIA_API_KEY"])()

    df = result.raw
    assert list(df.columns) == [c.name for c in EIA_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no datasets"
    assert (df["code"].str.startswith("coal/") | (df["code"] == "coal")).all(), "leaf routes not under coal"
    # Real manifest content, not just column names.
    assert df["measures"].str.len().gt(0).all(), "a dataset has no measures"
    assert df["description"].str.len().gt(0).all(), "blank descriptions"
    assert df["frequencies"].str.len().gt(0).any(), "no frequencies captured"

    # Entity projection round-trips on the real slice.
    entities = list(Result(raw=df, output_spec=EIA_ENUMERATE_OUTPUT).entities.values())
    assert len(entities) == len(df)
    assert entities[0].namespace == "eia"


def test_eia_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``eia_search`` end-to-end over a small, locally-built catalog so
    it runs network-free and never triggers the full route-tree enumerate."""
    rows = [
        {
            "code": "petroleum/pri/spt",
            "title": "Spot Prices",
            "description": (
                "Crude oil and petroleum product spot prices. petroleum energy data from the EIA. "
                "Measures: value ($/GAL). Facets: duoarea, product, process, series."
            ),
            "category": "petroleum",
            "measures": "value",
            "facets": "duoarea,product,process,series",
            "frequencies": "daily,weekly,monthly,annual",
            "default_frequency": "monthly",
            "start": "1986-01-02",
            "end": "2026-06-01",
            "units": "$/GAL",
        },
        {
            "code": "electricity/retail-sales",
            "title": "Electricity Retail Sales",
            "description": (
                "Electricity retail sales, revenue, price and customers by state and sector. "
                "electricity energy data from the EIA. Measures: revenue, sales, price, customers."
            ),
            "category": "electricity",
            "measures": "revenue,sales,price,customers",
            "facets": "stateid,sectorid",
            "frequencies": "monthly,quarterly,annual",
            "default_frequency": "monthly",
            "start": "2001-01",
            "end": "2026-03",
            "units": "cents/kWh",
        },
        {
            "code": "natural-gas/pri/fut",
            "title": "Natural Gas Futures",
            "description": (
                "Natural gas futures contract prices. natural-gas energy data from the EIA. "
                "Measures: value ($/MMBTU). Facets: duoarea, product, process, series."
            ),
            "category": "natural-gas",
            "measures": "value",
            "facets": "duoarea,product,process,series",
            "frequencies": "daily,weekly,monthly,annual",
            "default_frequency": "monthly",
            "start": "1994-01-04",
            "end": "2026-06-01",
            "units": "$/MMBTU",
        },
    ]
    df = pd.DataFrame(rows, columns=[c.name for c in EIA_ENUMERATE_OUTPUT.columns])
    entries = list(Result(raw=df, output_spec=EIA_ENUMERATE_OUTPUT).entities.values())
    catalog = Catalog("eia", indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "eia_catalog"
    catalog.save(out_dir)

    result = eia_search(query="electricity retail sales price", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="eia_search", required_param_keys=["query"])
    sdf = result.raw
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert sdf.iloc[0]["code"] == "electricity/retail-sales"

    # Ranking discriminates: a different query surfaces a different top hit.
    other = eia_search(query="natural gas futures price", limit=5, catalog_url=str(out_dir))
    assert other.raw.iloc[0]["code"] == "natural-gas/pri/fut"
