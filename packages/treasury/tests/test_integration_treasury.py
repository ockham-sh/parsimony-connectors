"""Live integration tests for parsimony-treasury.

Hits the real public Treasury sources — the Fiscal Data JSON API
(``api.fiscaldata.treasury.gov``) and the Office of Debt Management XML rate
feeds (``home.treasury.gov``). Treasury is **keyless**, so these tests need no
env vars and run in CI without secrets — there is no key to bind and no secret
that could leak (no ``assert_no_secret_leak`` needed).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/treasury -m integration

``treasury_search`` is covered against a **bounded** locally-built fixture
catalog rather than the published snapshot: the full live build embeds every
Fiscal Data measure (~900 rows) and is far too expensive for a test. We build a
3-row catalog from real enumerator-shaped rows and search it — the catalog
machinery is exercised end-to-end without the full embed.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.result import Result
from parsimony_test_support import assert_provenance_shape

from parsimony_treasury import (
    TREASURY_ENUMERATE_OUTPUT,
    enumerate_treasury,
    treasury_fetch,
    treasury_rates_fetch,
    treasury_search,
)
from parsimony_treasury.catalog_build import CATALOG_NAMESPACE

pytestmark = pytest.mark.integration


def test_treasury_fetch_debt_to_penny_live() -> None:
    # debt_to_penny is a stable, high-traffic daily Fiscal Data dataset.
    result = treasury_fetch(
        endpoint="v2/accounting/od/debt_to_penny",
        sort="-record_date",
        page_size=10,
    )

    assert_provenance_shape(result, expected_source="treasury_fetch", required_param_keys=["endpoint"])
    df = result.data
    assert not df.empty, "debt_to_penny returned an empty DataFrame"
    assert len(df) <= 10, "page_size not respected"
    assert list(df["endpoint"].unique()) == ["v2/accounting/od/debt_to_penny"]
    # Real content: record_date parses to real datetimes and the debt measure
    # is a real, large, coerced number — not a string or NaN.
    assert df["record_date"].notna().any(), "record_date is all NaT"
    assert df["record_date"].dtype.kind == "M"
    assert df["tot_pub_debt_out_amt"].dtype.kind == "f", "currency column not coerced to float"
    assert df["tot_pub_debt_out_amt"].notna().any(), "no real debt value"
    # US federal debt is in the tens of trillions — sanity-check the magnitude.
    assert df["tot_pub_debt_out_amt"].max() > 1e13, "debt magnitude implausibly small"


def test_treasury_rates_fetch_yield_curve_live() -> None:
    # 2024 is a complete, stable year for the par yield curve feed (XML path).
    result = treasury_rates_fetch(feed="daily_treasury_yield_curve", year=2024)

    assert_provenance_shape(result, expected_source="treasury_rates_fetch", required_param_keys=["feed"])
    df = result.data
    assert not df.empty, "yield curve feed returned an empty DataFrame"
    # Real content from the XML: the 10-year tenor must carry real numeric rates.
    assert "BC_10YEAR" in df.columns, "native rate column BC_10YEAR missing"
    assert df["BC_10YEAR"].notna().any(), "no real 10-year yield values"
    bc10 = df["BC_10YEAR"].dropna()
    assert ((bc10 > 0) & (bc10 < 25)).all(), f"10-year yields out of plausible range: {bc10.tolist()[:5]}"
    # record_date is the normalised, sorted time axis cloned from NEW_DATE.
    assert df["record_date"].notna().any(), "record_date is all NaT"
    assert df["record_date"].is_monotonic_increasing, "record_date not sorted ascending"
    assert df["record_date"].dt.year.eq(2024).all(), "rows not from the requested year"
    assert list(df["feed"].unique()) == ["daily_treasury_yield_curve"]


def test_treasury_rates_fetch_bill_rates_live() -> None:
    # A second feed whose date column is INDEX_DATE (not NEW_DATE) — exercises
    # the alternate date-column branch of the XML parser against the real feed.
    result = treasury_rates_fetch(feed="daily_treasury_bill_rates", year=2024)

    df = result.data
    assert not df.empty, "bill rates feed returned an empty DataFrame"
    assert "ROUND_B1_YIELD_4WK_2" in df.columns, "native 4-week bill yield column missing"
    assert df["ROUND_B1_YIELD_4WK_2"].notna().any(), "no real 4-week bill yields"
    # record_date must resolve even though this feed uses INDEX_DATE/QUOTE_DATE.
    assert df["record_date"].notna().any(), "record_date unresolved for bill rates feed"
    assert df["record_date"].dtype.kind == "M"


def test_rate_feed_registry_has_no_live_phantom() -> None:
    """Cross-validate the curated ODM registry against the live feed columns (the
    archetype-D discipline; mirrors scripts/harvest_rate_feeds.py).

    2025 carries the **current full** maturity set — including the 1.5-month par point and
    the 6-week bill, both added in 2025. Every registry benchmark column must appear in the
    live 2025 feed (a stale phantom would fail here). NOTE: these columns are sparse — OData
    omits null properties per-entry, so the check must use the column UNION across all rows
    (the DataFrame ``columns``), never a single entry."""
    from parsimony_treasury.rate_feeds import registry_columns

    for feed in ("daily_treasury_yield_curve", "daily_treasury_real_yield_curve", "daily_treasury_bill_rates"):
        result = treasury_rates_fetch(feed=feed, year=2025)
        live_cols = set(result.data.columns)
        missing = registry_columns(feed) - live_cols
        assert not missing, f"{feed}: registry columns absent from the 2025 live feed (phantom?): {sorted(missing)}"


def test_enumerate_treasury_live() -> None:
    # ONE metadata GET + an in-memory fan-out over the returned datasets. The
    # network cost is a single request; we do NOT embed/build a catalog here.
    result = enumerate_treasury()

    df = result.data
    # @enumerator: exact column match against the declared schema.
    assert list(df.columns) == [c.name for c in TREASURY_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "enumeration returned no rows"

    fiscal = df[df["source"] == "fiscal_data"]
    rates = df[df["source"] == "treasury_rates"]
    assert len(fiscal) > 100, "implausibly few Fiscal Data measures enumerated"
    # The static ODM rate registry must always land (does not depend on the API).
    assert len(rates) > 30, "ODM rate-feed registry rows missing"

    # Real content, not just column names: code/title/description are populated.
    assert df["code"].astype(str).str.len().gt(0).all(), "blank code"
    assert df["title"].astype(str).str.len().gt(0).all(), "blank title"
    assert df["description"].astype(str).str.len().gt(0).any(), "no real description text"
    # The canonical debt measure is discoverable.
    assert df["code"].str.contains("debt_to_penny").any(), "debt_to_penny not enumerated"
    # Entity projection round-trips on a real slice (catalog-build entry point).
    entities = Result(data=df.head(20), output_spec=TREASURY_ENUMERATE_OUTPUT).to_entities()
    assert len(entities) == 20
    assert entities[0].namespace == CATALOG_NAMESPACE


def test_treasury_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise the search verb end-to-end over a small, locally-built catalog.

    Bounded by design: a full live ``build_treasury_catalog()`` embeds ~900
    measure rows. We build a 3-row catalog from real enumerator-shaped rows,
    persist it, and search it — covering load + rank + result shaping without
    the expensive embed.
    """
    rows = [
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
        {
            "code": "home/daily_treasury_yield_curve#BC_10YEAR",
            "title": "10 Year — Daily Treasury Par Yield Curve Rates",
            "description": "10 Year constant-maturity Treasury par yield curve rate.",
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
            "code": "v1/accounting/dts/operating_cash_balance#open_today_bal",
            "title": "Opening Balance — Operating Cash Balance",
            "description": "Treasury operating cash opening balance for the day.",
            "source": "fiscal_data",
            "endpoint": "v1/accounting/dts/operating_cash_balance",
            "field": "open_today_bal",
            "data_type": "CURRENCY",
            "dataset": "Daily Treasury Statement",
            "category": "Bureau of the Fiscal Service",
            "frequency": "Daily",
            "earliest_date": "2005-10-03",
            "latest_date": "2026-06-01",
        },
    ]
    df = pd.DataFrame(rows, columns=[c.name for c in TREASURY_ENUMERATE_OUTPUT.columns])
    entries = Result(data=df, output_spec=TREASURY_ENUMERATE_OUTPUT).to_entities()
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "treasury_catalog"
    catalog.save(out_dir)

    result = treasury_search(query="10 year treasury yield curve", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="treasury_search", required_param_keys=["query"])
    sdf = result.data
    assert list(sdf.columns) == ["code", "title", "score"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "result not bounded by the fixture catalog"
    # Real ranking: the yield-curve entry is the top hit and scores are populated.
    assert sdf.iloc[0]["code"] == "home/daily_treasury_yield_curve#BC_10YEAR"
    assert sdf["score"].notna().all(), "search scores not populated"
