"""Happy-path tests for the US Treasury Fiscal Data connectors.

Follows ``docs/testing-template.md``: respx-mocked upstream, assertions limited to
the public ``Result`` surface. Treasury has no ``api_key`` dep, so the
401/429 error-mapping tests in the template do not apply here.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_treasury import (
    CONNECTORS,
    TreasuryEnumerateParams,
    TreasuryFetchParams,
    TreasuryRatesFetchParams,
    enumerate_treasury,
    treasury_fetch,
    treasury_rates_fetch,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"treasury_fetch", "treasury_rates_fetch", "enumerate_treasury", "treasury_search"}


def test_treasury_fetch_tags() -> None:
    fetch = next(c for c in CONNECTORS if c.name == "treasury_fetch")
    assert {"macro", "us"} <= set(fetch.tags)


# ---------------------------------------------------------------------------
# treasury_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_treasury_fetch_returns_records() -> None:
    respx.get(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "record_date": "2026-01-02",
                        "tot_pub_debt_out_amt": "34100000000000",
                        "debt_held_public_amt": "27000000000000",
                        "intragov_hold_amt": "7100000000000",
                    },
                ],
                "meta": {
                    "total-count": 1,
                    "labels": {"record_date": "Debt to the Penny"},
                    "dataTypes": {
                        "tot_pub_debt_out_amt": "CURRENCY",
                        "debt_held_public_amt": "CURRENCY",
                        "intragov_hold_amt": "CURRENCY",
                    },
                },
            },
        )
    )

    result = await treasury_fetch(TreasuryFetchParams(endpoint="v2/accounting/od/debt_to_penny"))

    assert result.provenance.source == "treasury"
    assert result.provenance.params == {"endpoint": "v2/accounting/od/debt_to_penny"}
    df = result.data
    assert "record_date" in df.columns
    assert "endpoint" in df.columns
    assert list(df["endpoint"]) == ["v2/accounting/od/debt_to_penny"]


@respx.mock
@pytest.mark.asyncio
async def test_treasury_fetch_raises_empty_data_on_no_records() -> None:
    respx.get(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
    ).mock(return_value=httpx.Response(200, json={"data": [], "meta": {}}))

    with pytest.raises(EmptyDataError):
        await treasury_fetch(TreasuryFetchParams(endpoint="v2/accounting/od/debt_to_penny"))


# ---------------------------------------------------------------------------
# treasury_rates_fetch (home.treasury.gov OData/Atom XML)
# ---------------------------------------------------------------------------


_YIELD_CURVE_XML = (
    '<?xml version="1.0" encoding="utf-8" standalone="yes" ?>'
    '<feed xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"'
    ' xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"'
    ' xmlns="http://www.w3.org/2005/Atom">'
    "<entry><content type=\"application/xml\"><m:properties>"
    '<d:Id m:type="Edm.Int32">140</d:Id>'
    '<d:NEW_DATE m:type="Edm.DateTime">2026-01-02T00:00:00</d:NEW_DATE>'
    '<d:BC_1MONTH m:type="Edm.Double">3.72</d:BC_1MONTH>'
    '<d:BC_10YEAR m:type="Edm.Double">4.19</d:BC_10YEAR>'
    '<d:BC_30YEAR m:type="Edm.Double">4.86</d:BC_30YEAR>'
    "</m:properties></content></entry>"
    "<entry><content type=\"application/xml\"><m:properties>"
    '<d:Id m:type="Edm.Int32">141</d:Id>'
    '<d:NEW_DATE m:type="Edm.DateTime">2026-01-03T00:00:00</d:NEW_DATE>'
    '<d:BC_1MONTH m:type="Edm.Double">3.71</d:BC_1MONTH>'
    '<d:BC_10YEAR m:type="Edm.Double">4.20</d:BC_10YEAR>'
    '<d:BC_30YEAR m:type="Edm.Double">4.87</d:BC_30YEAR>'
    "</m:properties></content></entry>"
    "</feed>"
)


@respx.mock
@pytest.mark.asyncio
async def test_treasury_rates_fetch_parses_xml_and_normalises_record_date() -> None:
    respx.get("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml").mock(
        return_value=httpx.Response(200, text=_YIELD_CURVE_XML, headers={"content-type": "text/xml"})
    )

    result = await treasury_rates_fetch(
        TreasuryRatesFetchParams(feed="daily_treasury_yield_curve", year=2026)
    )

    df = result.data
    assert len(df) == 2
    # Native rate columns are preserved; numeric typing applied.
    assert df["BC_10YEAR"].tolist() == [4.19, 4.20]
    # ``record_date`` is the normalised time axis cloned from NEW_DATE.
    assert "record_date" in df.columns
    assert df["record_date"].iloc[0] == pd.Timestamp("2026-01-02")
    assert df["record_date"].iloc[-1] == pd.Timestamp("2026-01-03")
    # Identity columns added for catalog interop.
    assert list(df["feed"].unique()) == ["daily_treasury_yield_curve"]
    assert result.provenance.params == {"feed": "daily_treasury_yield_curve", "year": 2026}
    assert "field_tdr_date_value=2026" in result.provenance.properties["source_url"]


@respx.mock
@pytest.mark.asyncio
async def test_treasury_rates_fetch_raises_empty_data_on_no_entries() -> None:
    empty_xml = (
        '<?xml version="1.0" encoding="utf-8" standalone="yes" ?>'
        '<feed xmlns="http://www.w3.org/2005/Atom"></feed>'
    )
    respx.get("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml").mock(
        return_value=httpx.Response(200, text=empty_xml, headers={"content-type": "text/xml"})
    )

    with pytest.raises(EmptyDataError):
        await treasury_rates_fetch(
            TreasuryRatesFetchParams(feed="daily_treasury_yield_curve", year=1990)
        )


def test_treasury_rates_fetch_params_reject_unknown_feed() -> None:
    with pytest.raises(ValueError):
        TreasuryRatesFetchParams(feed="bogus_feed")  # type: ignore[arg-type]


def test_treasury_rates_fetch_params_year_bounds() -> None:
    with pytest.raises(ValueError):
        TreasuryRatesFetchParams(feed="daily_treasury_yield_curve", year=1989)
    with pytest.raises(ValueError):
        TreasuryRatesFetchParams(feed="daily_treasury_yield_curve", year=2101)


# ---------------------------------------------------------------------------
# enumerate_treasury
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_treasury_emits_one_row_per_measure_field() -> None:
    respx.get("https://api.fiscaldata.treasury.gov/services/dtg/metadata/").mock(
        return_value=httpx.Response(
            200,
            json={
                "datasets": [
                    {
                        "title": "Debt to the Penny",
                        "dataset_name": "Debt to the Penny",
                        "publisher": "Bureau of the Fiscal Service",
                        "update_frequency": "Daily",
                        "apis": [
                            {
                                "endpoint_txt": "/services/api/fiscal_service/v2/accounting/od/debt_to_penny",
                                "table_name": "Debt to the Penny",
                                "api_id": "debt_to_penny",
                                "earliest_date": "1993-04-01",
                                "latest_date": "2026-04-22",
                                "fields": [
                                    # Non-measure: date — skipped.
                                    {
                                        "column_name": "record_date",
                                        "pretty_name": "Record Date",
                                        "data_type": "DATE",
                                        "definition": "Publication date.",
                                    },
                                    # Measure: currency — kept.
                                    {
                                        "column_name": "tot_pub_debt_out_amt",
                                        "pretty_name": "Total Public Debt Outstanding",
                                        "data_type": "CURRENCY",
                                        "definition": "All federal debt.",
                                    },
                                    # Measure with precision suffix — kept via prefix match.
                                    {
                                        "column_name": "debt_held_public_amt",
                                        "pretty_name": "Debt Held by the Public",
                                        "data_type": "CURRENCY0",
                                        "definition": "Debt held outside the government.",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        )
    )

    result = await enumerate_treasury(TreasuryEnumerateParams())

    df = result.data
    fiscal = df[~df["endpoint"].str.startswith("home/")]
    assert len(fiscal) == 2, "one row per measure field (DATE is filtered out)"
    assert set(fiscal["code"]) == {
        "v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt",
        "v2/accounting/od/debt_to_penny#debt_held_public_amt",
    }
    # Title combines the field's pretty_name with the API table_name so the
    # embedder sees both the specific measure and its dataset context.
    total_pub = fiscal[fiscal["field"] == "tot_pub_debt_out_amt"].iloc[0]
    assert total_pub["title"] == "Total Public Debt Outstanding — Debt to the Penny"
    assert total_pub["endpoint"] == "v2/accounting/od/debt_to_penny"
    assert total_pub["definition"] == "All federal debt."
    assert total_pub["data_type"] == "CURRENCY"
    assert total_pub["frequency"] == "Daily"
    assert total_pub["earliest_date"] == "1993-04-01"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_treasury_keeps_tcir_string_rate_fields() -> None:
    """TCIR (Certified Interest Rates) tables store their rate values as STRING
    in Fiscal Data's data dictionary. The enumerator must include them anyway
    by falling back to a name-based heuristic on STRING fields."""
    respx.get("https://api.fiscaldata.treasury.gov/services/dtg/metadata/").mock(
        return_value=httpx.Response(
            200,
            json={
                "datasets": [
                    {
                        "title": "Treasury Certified Interest Rates: Monthly Certification",
                        "publisher": "Bureau of the Fiscal Service",
                        "update_frequency": "Monthly",
                        "apis": [
                            {
                                "endpoint_txt": "/services/api/fiscal_service/v1/accounting/od/tcir_monthly_table_2",
                                "table_name": "TCIR Monthly Table 2",
                                "fields": [
                                    {
                                        "column_name": "monthly_rate",
                                        "pretty_name": "Rate",
                                        "data_type": "STRING",
                                        "definition": "Certified interest rate for the month.",
                                    },
                                    {
                                        "column_name": "rate_desc",
                                        "pretty_name": "Description",
                                        "data_type": "STRING",
                                        "definition": "The description for which the certified interest rates are applicable.",  # noqa: E501
                                    },
                                    {
                                        "column_name": "floating_rate",
                                        "pretty_name": "Floating Rate",
                                        "data_type": "STRING",
                                        "definition": "Y/N field that tells investors if it's a floating rate note.",
                                    },
                                    {
                                        "column_name": "record_date",
                                        "pretty_name": "Record Date",
                                        "data_type": "DATE",
                                        "definition": "Record date.",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        )
    )

    result = await enumerate_treasury(TreasuryEnumerateParams())
    df = result.data
    fiscal = df[~df["endpoint"].str.startswith("home/")]
    # Only the genuine rate-bearing string field is kept; *_desc and Y/N flags
    # are filtered out, dates remain excluded.
    assert set(fiscal["code"]) == {"v1/accounting/od/tcir_monthly_table_2#monthly_rate"}
    row = fiscal.iloc[0]
    assert row["data_type"] == "STRING"
    assert row["dataset"] == "Treasury Certified Interest Rates: Monthly Certification"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_treasury_appends_office_of_debt_management_rates() -> None:
    """The static rate-feed registry contributes catalog rows on every run,
    independent of the Fiscal Data metadata. Codes use a ``home/`` prefix to
    distinguish them from versioned Fiscal Data endpoints."""
    respx.get("https://api.fiscaldata.treasury.gov/services/dtg/metadata/").mock(
        return_value=httpx.Response(200, json={"datasets": []})
    )

    result = await enumerate_treasury(TreasuryEnumerateParams())
    df = result.data
    home = df[df["endpoint"].str.startswith("home/")]
    assert len(home) > 30, "expected the full rate-feed registry to land"

    # Canonical par yield curve 10-year tenor.
    par_10y = home[home["code"] == "home/daily_treasury_yield_curve#BC_10YEAR"]
    assert not par_10y.empty
    par_10y_row = par_10y.iloc[0]
    assert par_10y_row["dataset"] == "Daily Treasury Par Yield Curve Rates"
    assert par_10y_row["title"] == "10 Year — Daily Treasury Par Yield Curve Rates"
    assert "constant-maturity" in par_10y_row["definition"]
    assert par_10y_row["frequency"] == "Daily"
    assert par_10y_row["source"] == "treasury_rates"

    # Real yield curve, bill rates, long-term, real long-term — all five
    # families should be represented.
    feeds = set(home["endpoint"])
    assert {
        "home/daily_treasury_yield_curve",
        "home/daily_treasury_real_yield_curve",
        "home/daily_treasury_bill_rates",
        "home/daily_treasury_long_term_rate",
        "home/daily_treasury_real_long_term",
    }.issubset(feeds)


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_treasury_emits_source_metadata_for_dispatch() -> None:
    """Every row carries ``source`` so an agent dispatching off a search hit
    knows which fetch connector to call without parsing the code prefix."""
    respx.get("https://api.fiscaldata.treasury.gov/services/dtg/metadata/").mock(
        return_value=httpx.Response(
            200,
            json={
                "datasets": [
                    {
                        "title": "Debt to the Penny",
                        "publisher": "Bureau of the Fiscal Service",
                        "update_frequency": "Daily",
                        "apis": [
                            {
                                "endpoint_txt": "/services/api/fiscal_service/v2/accounting/od/debt_to_penny",
                                "table_name": "Debt to the Penny",
                                "fields": [
                                    {
                                        "column_name": "tot_pub_debt_out_amt",
                                        "pretty_name": "Total Public Debt Outstanding",
                                        "data_type": "CURRENCY",
                                        "definition": "All federal debt.",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        )
    )

    df = (await enumerate_treasury(TreasuryEnumerateParams())).data
    fiscal_sources = set(df[df["endpoint"] == "v2/accounting/od/debt_to_penny"]["source"])
    rates_sources = set(df[df["endpoint"].str.startswith("home/")]["source"])
    assert fiscal_sources == {"fiscal_data"}
    assert rates_sources == {"treasury_rates"}


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_treasury_fetch_params_bounds_page_size() -> None:
    with pytest.raises(ValueError):
        TreasuryFetchParams(endpoint="x", page_size=0)
    with pytest.raises(ValueError):
        TreasuryFetchParams(endpoint="x", page_size=10001)


def test_treasury_fetch_params_accepts_filter_and_sort() -> None:
    p = TreasuryFetchParams(
        endpoint="v2/accounting/od/debt_to_penny",
        filter="record_date:gte:2026-01-01",
        sort="-record_date",
        page_size=50,
    )
    assert p.filter == "record_date:gte:2026-01-01"
    assert p.sort == "-record_date"
