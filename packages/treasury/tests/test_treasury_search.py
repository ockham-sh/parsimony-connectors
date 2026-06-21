"""Catalog search relevance tests for parsimony-treasury (fixture-backed, no network)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_treasury import TREASURY_ENUMERATE_OUTPUT, treasury_search
from parsimony_treasury.catalog_build import CATALOG_NAMESPACE


def _build_fixture_catalog(tmp_path: Path) -> Path:
    rows = [
        {
            "code": "home/daily_treasury_yield_curve#BC_10YEAR",
            "title": "10 Year — Daily Treasury Par Yield Curve Rates",
            "description": (
                "10 Year constant-maturity Treasury par yield curve rate, published daily by the "
                "U.S. Treasury Office of Debt Management. The par yield curve is derived from "
                "indicative bid-side prices on the most actively traded Treasury securities."
            ),
            "source": "treasury_rates",
            "endpoint": "home/daily_treasury_yield_curve",
            "field": "BC_10YEAR",
            "data_type": "PERCENTAGE",
            "dataset": "Daily Treasury Par Yield Curve Rates",
            "category": "Office of Debt Management",
            "frequency": "Daily",
            "earliest_date": "",
            "latest_date": "",
        },
        {
            "code": "home/daily_treasury_real_yield_curve#TC_10YEAR",
            "title": "10 Year — Daily Treasury Real Yield Curve Rates",
            "description": (
                "10 Year real (TIPS-based) Treasury yield curve rate, published daily by the U.S. "
                "Treasury Office of Debt Management. Reflects the inflation-adjusted yield on "
                "Treasury Inflation-Protected Securities at the given constant maturity."
            ),
            "source": "treasury_rates",
            "endpoint": "home/daily_treasury_real_yield_curve",
            "field": "TC_10YEAR",
            "data_type": "PERCENTAGE",
            "dataset": "Daily Treasury Real Yield Curve Rates",
            "category": "Office of Debt Management",
            "frequency": "Daily",
            "earliest_date": "",
            "latest_date": "",
        },
        {
            "code": "v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt",
            "title": "Total Public Debt Outstanding — Debt to the Penny",
            "description": "Total federal debt outstanding to the penny.",
            "source": "fiscal_data",
            "endpoint": "v2/accounting/od/debt_to_penny",
            "field": "tot_pub_debt_out_amt",
            "data_type": "CURRENCY",
            "dataset": "Debt to the Penny",
            "category": "Bureau of the Fiscal Service",
            "frequency": "Daily",
            "earliest_date": "1993-04-01",
            "latest_date": "2026-06-01",
        },
    ]
    df = pd.DataFrame(rows, columns=[c.name for c in TREASURY_ENUMERATE_OUTPUT.columns])
    entries = entities_from_raw(df, TREASURY_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "treasury_catalog"
    catalog.save(out_dir)
    return out_dir


@pytest.fixture
def treasury_catalog_dir(tmp_path: Path) -> Path:
    return _build_fixture_catalog(tmp_path)


def test_treasury_search_title_query_ranks_yield_curve(treasury_catalog_dir: Path) -> None:
    result = treasury_search(query="10 year treasury yield curve", limit=5, catalog_url=str(treasury_catalog_dir))
    assert result.data.iloc[0]["code"] == "home/daily_treasury_yield_curve#BC_10YEAR"


def test_treasury_search_description_query_finds_par_yield(treasury_catalog_dir: Path) -> None:
    result = treasury_search(
        query="description: constant maturity Treasury par yield",
        limit=5,
        catalog_url=str(treasury_catalog_dir),
    )
    codes = set(result.data["code"])
    assert "home/daily_treasury_yield_curve#BC_10YEAR" in codes


def test_treasury_search_description_query_finds_tips_real_yield(treasury_catalog_dir: Path) -> None:
    result = treasury_search(
        query="description: Treasury Inflation-Protected Securities",
        limit=5,
        catalog_url=str(treasury_catalog_dir),
    )
    assert result.data.iloc[0]["code"] == "home/daily_treasury_real_yield_curve#TC_10YEAR"


def test_treasury_search_code_prefix_exact_match(treasury_catalog_dir: Path) -> None:
    result = treasury_search(
        query="code: home/daily_treasury_yield_curve#BC_10YEAR",
        limit=5,
        catalog_url=str(treasury_catalog_dir),
    )
    assert result.data.iloc[0]["code"] == "home/daily_treasury_yield_curve#BC_10YEAR"
    assert result.data.iloc[0]["score"] >= 1_000_000


def test_treasury_search_returns_dispatch_metadata(treasury_catalog_dir: Path) -> None:
    result = treasury_search(query="10 year treasury yield curve", limit=3, catalog_url=str(treasury_catalog_dir))
    row = result.data.iloc[0]
    assert row["source"] == "treasury_rates"
    assert row["endpoint"] == "home/daily_treasury_yield_curve"
    assert row["field"] == "BC_10YEAR"
