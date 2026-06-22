"""Offline tests for the US Treasury connectors.

respx-mocked upstream; assertions limited to the public ``Result`` surface and
inline-validation error paths. Treasury is keyless — no ``api_key`` dep, so the
401/429 credential-mapping tests in the template do not apply here.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_treasury import (
    CONNECTORS,
    TREASURY_ENUMERATE_OUTPUT,
    enumerate_treasury,
    treasury_fetch,
    treasury_rates_fetch,
)

_FISCAL_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
_METADATA_URL = "https://api.fiscaldata.treasury.gov/services/dtg/metadata/"
_RATES_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"treasury_fetch", "treasury_rates_fetch", "enumerate_treasury", "treasury_search"}


def test_treasury_fetch_tags() -> None:
    fetch = next(c for c in CONNECTORS if c.name == "treasury_fetch")
    assert {"macro", "us"} <= set(fetch.tags)


def test_treasury_fetch_namespace_hint() -> None:
    assert dict(treasury_fetch.namespace_hints) == {"endpoint": "treasury"}
    assert dict(treasury_rates_fetch.namespace_hints) == {"feed": "treasury"}


# ---------------------------------------------------------------------------
# treasury_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_treasury_fetch_returns_records() -> None:
    respx.get(f"{_FISCAL_BASE}/v2/accounting/od/debt_to_penny").mock(
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

    result = treasury_fetch(endpoint="v2/accounting/od/debt_to_penny")

    assert result.provenance.source == "treasury_fetch"
    assert result.provenance.params == {
        "endpoint": "v2/accounting/od/debt_to_penny",
        "filter": None,
        "sort": None,
        "page_size": 100,
    }
    df = result.data
    assert "record_date" in df.columns
    assert list(df["endpoint"]) == ["v2/accounting/od/debt_to_penny"]
    assert list(df["title"]) == ["Debt to the Penny"]
    # Metadata-typed numeric columns are coerced from strings to numbers
    # (comma-stripped); pandas may pick int or float depending on the values.
    assert df["tot_pub_debt_out_amt"].dtype.kind in "if"
    assert df["tot_pub_debt_out_amt"].iloc[0] == 34100000000000


@respx.mock
def test_treasury_fetch_strips_leading_slash_in_endpoint() -> None:
    route = respx.get(f"{_FISCAL_BASE}/v2/accounting/od/debt_to_penny").mock(
        return_value=httpx.Response(200, json={"data": [{"record_date": "2026-01-02"}], "meta": {}})
    )
    result = treasury_fetch(endpoint="/v2/accounting/od/debt_to_penny")
    assert route.called
    assert list(result.data["endpoint"]) == ["v2/accounting/od/debt_to_penny"]


@respx.mock
def test_treasury_fetch_raises_empty_data_on_no_records() -> None:
    respx.get(f"{_FISCAL_BASE}/v2/accounting/od/debt_to_penny").mock(
        return_value=httpx.Response(200, json={"data": [], "meta": {}})
    )
    with pytest.raises(EmptyDataError) as exc:
        treasury_fetch(endpoint="v2/accounting/od/debt_to_penny")
    assert exc.value.query_params == {
        "endpoint": "v2/accounting/od/debt_to_penny",
        "filter": None,
        "sort": None,
    }


@respx.mock
def test_treasury_fetch_raises_parse_error_on_missing_data_key() -> None:
    # HTTP 200 but not the expected shape (no 'data') -> ParseError, not a fake status.
    respx.get(f"{_FISCAL_BASE}/v2/accounting/od/debt_to_penny").mock(
        return_value=httpx.Response(200, json={"error": "nope"})
    )
    with pytest.raises(ParseError):
        treasury_fetch(endpoint="v2/accounting/od/debt_to_penny")


@respx.mock
def test_treasury_fetch_maps_http_error() -> None:
    from parsimony.errors import ProviderError

    respx.get(f"{_FISCAL_BASE}/v2/accounting/od/bad").mock(return_value=httpx.Response(503))
    with pytest.raises(ProviderError) as exc:
        treasury_fetch(endpoint="v2/accounting/od/bad")
    assert exc.value.status_code == 503


def test_treasury_fetch_rejects_empty_endpoint() -> None:
    with pytest.raises(InvalidParameterError):
        treasury_fetch(endpoint="   ")


def test_treasury_fetch_rejects_out_of_range_page_size() -> None:
    with pytest.raises(InvalidParameterError):
        treasury_fetch(endpoint="v2/accounting/od/debt_to_penny", page_size=0)
    with pytest.raises(InvalidParameterError):
        treasury_fetch(endpoint="v2/accounting/od/debt_to_penny", page_size=10001)


@respx.mock
def test_treasury_fetch_passes_filter_and_sort() -> None:
    route = respx.get(f"{_FISCAL_BASE}/v2/accounting/od/debt_to_penny").mock(
        return_value=httpx.Response(200, json={"data": [{"record_date": "2026-01-02"}], "meta": {}})
    )
    treasury_fetch(
        endpoint="v2/accounting/od/debt_to_penny",
        filter="record_date:gte:2026-01-01",
        sort="-record_date",
        page_size=50,
    )
    sent = route.calls.last.request
    assert sent.url.params["filter"] == "record_date:gte:2026-01-01"
    assert sent.url.params["sort"] == "-record_date"
    assert sent.url.params["page[size]"] == "50"
    assert sent.url.params["format"] == "json"


# ---------------------------------------------------------------------------
# treasury_rates_fetch (home.treasury.gov OData/Atom XML)
# ---------------------------------------------------------------------------


_YIELD_CURVE_XML = (
    '<?xml version="1.0" encoding="utf-8" standalone="yes" ?>'
    '<feed xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"'
    ' xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"'
    ' xmlns="http://www.w3.org/2005/Atom">'
    '<entry><content type="application/xml"><m:properties>'
    '<d:Id m:type="Edm.Int32">140</d:Id>'
    '<d:NEW_DATE m:type="Edm.DateTime">2026-01-03T00:00:00</d:NEW_DATE>'
    '<d:BC_1MONTH m:type="Edm.Double">3.72</d:BC_1MONTH>'
    '<d:BC_10YEAR m:type="Edm.Double">4.19</d:BC_10YEAR>'
    '<d:BC_30YEAR m:type="Edm.Double">4.86</d:BC_30YEAR>'
    "</m:properties></content></entry>"
    '<entry><content type="application/xml"><m:properties>'
    '<d:Id m:type="Edm.Int32">139</d:Id>'
    '<d:NEW_DATE m:type="Edm.DateTime">2026-01-02T00:00:00</d:NEW_DATE>'
    '<d:BC_1MONTH m:type="Edm.Double">3.71</d:BC_1MONTH>'
    '<d:BC_10YEAR m:type="Edm.Double">4.20</d:BC_10YEAR>'
    '<d:BC_30YEAR m:type="Edm.Double">4.87</d:BC_30YEAR>'
    "</m:properties></content></entry>"
    "</feed>"
)


@respx.mock
def test_treasury_rates_fetch_parses_xml_and_normalises_record_date() -> None:
    respx.get(_RATES_URL).mock(
        return_value=httpx.Response(200, text=_YIELD_CURVE_XML, headers={"content-type": "text/xml"})
    )

    result = treasury_rates_fetch(feed="daily_treasury_yield_curve", year=2026)

    df = result.data
    assert len(df) == 2
    # Native rate columns are preserved; numeric typing applied.
    assert df["BC_10YEAR"].tolist() == [4.20, 4.19]
    # ``record_date`` is the normalised time axis cloned from NEW_DATE, sorted ascending.
    assert "record_date" in df.columns
    assert df["record_date"].iloc[0] == pd.Timestamp("2026-01-02")
    assert df["record_date"].iloc[-1] == pd.Timestamp("2026-01-03")
    # Identity columns added for catalog interop.
    assert list(df["feed"].unique()) == ["daily_treasury_yield_curve"]
    assert result.provenance.source == "treasury_rates_fetch"
    assert result.provenance.params == {"feed": "daily_treasury_yield_curve", "year": 2026}
    assert "field_tdr_date_value=2026" in str(df["source_url"].iloc[0])


@respx.mock
def test_treasury_rates_fetch_sends_feed_and_year_params() -> None:
    route = respx.get(_RATES_URL).mock(
        return_value=httpx.Response(200, text=_YIELD_CURVE_XML, headers={"content-type": "text/xml"})
    )
    treasury_rates_fetch(feed="daily_treasury_yield_curve", year=2024)
    sent = route.calls.last.request
    assert sent.url.params["data"] == "daily_treasury_yield_curve"
    assert sent.url.params["field_tdr_date_value"] == "2024"


@respx.mock
def test_treasury_rates_fetch_raises_empty_data_on_no_entries() -> None:
    empty_xml = (
        '<?xml version="1.0" encoding="utf-8" standalone="yes" ?><feed xmlns="http://www.w3.org/2005/Atom"></feed>'
    )
    respx.get(_RATES_URL).mock(return_value=httpx.Response(200, text=empty_xml, headers={"content-type": "text/xml"}))
    with pytest.raises(EmptyDataError):
        treasury_rates_fetch(feed="daily_treasury_yield_curve", year=1990)


@respx.mock
def test_treasury_rates_fetch_raises_parse_error_on_bad_xml() -> None:
    # HTTP 200 but the body is not parseable XML -> ParseError (200-with-error-body, §5.8).
    respx.get(_RATES_URL).mock(
        return_value=httpx.Response(200, text="<html><body>service unavailable", headers={"content-type": "text/html"})
    )
    with pytest.raises(ParseError):
        treasury_rates_fetch(feed="daily_treasury_yield_curve", year=2026)


@respx.mock
def test_treasury_rates_fetch_maps_http_error() -> None:
    from parsimony.errors import ProviderError

    respx.get(_RATES_URL).mock(return_value=httpx.Response(500))
    with pytest.raises(ProviderError) as exc:
        treasury_rates_fetch(feed="daily_treasury_yield_curve", year=2026)
    assert exc.value.status_code == 500


def test_treasury_rates_fetch_rejects_unknown_feed() -> None:
    with pytest.raises(InvalidParameterError):
        treasury_rates_fetch(feed="bogus_feed")  # type: ignore[arg-type]


def test_treasury_rates_fetch_rejects_out_of_range_year() -> None:
    with pytest.raises(InvalidParameterError):
        treasury_rates_fetch(feed="daily_treasury_yield_curve", year=1989)
    with pytest.raises(InvalidParameterError):
        treasury_rates_fetch(feed="daily_treasury_yield_curve", year=2101)


# ---------------------------------------------------------------------------
# enumerate_treasury
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_treasury_emits_one_row_per_measure_field() -> None:
    respx.get(_METADATA_URL).mock(
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

    result = enumerate_treasury()

    df = result.data
    # @enumerator enforces an EXACT column match — the frame carries exactly the declared columns.
    assert list(df.columns) == [c.name for c in TREASURY_ENUMERATE_OUTPUT.columns]
    fiscal = df[df["source"] == "fiscal_data"]
    assert len(fiscal) == 2, "one row per measure field (DATE is filtered out)"
    assert set(fiscal["code"]) == {
        "v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt",
        "v2/accounting/od/debt_to_penny#debt_held_public_amt",
    }
    total_pub = fiscal[fiscal["field"] == "tot_pub_debt_out_amt"].iloc[0]
    assert total_pub["title"] == "Total Public Debt Outstanding — Debt to the Penny"
    assert total_pub["endpoint"] == "v2/accounting/od/debt_to_penny"
    # The upstream Fiscal Data ``definition`` field text is carried in our indexed
    # ``description`` column (named so discovery_indexes actually indexes it).
    assert total_pub["description"] == "All federal debt."
    assert total_pub["data_type"] == "CURRENCY"
    assert total_pub["frequency"] == "Daily"
    assert total_pub["earliest_date"] == "1993-04-01"
    assert total_pub["source"] == "fiscal_data"


@respx.mock
def test_enumerate_treasury_handles_top_level_list_payload() -> None:
    # The live metadata endpoint returns a top-level JSON *list*, not an object.
    respx.get(_METADATA_URL).mock(
        return_value=httpx.Response(
            200,
            json=[
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
        )
    )

    df = (enumerate_treasury()).data
    fiscal = df[df["source"] == "fiscal_data"]
    assert set(fiscal["code"]) == {"v2/accounting/od/debt_to_penny#tot_pub_debt_out_amt"}


@respx.mock
def test_enumerate_treasury_keeps_tcir_string_rate_fields() -> None:
    """TCIR (Certified Interest Rates) tables store their rate values as STRING
    in Fiscal Data's data dictionary. The enumerator must include them anyway
    via a name-based heuristic on STRING fields, while excluding *_desc and Y/N
    indicator columns."""
    respx.get(_METADATA_URL).mock(
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

    df = (enumerate_treasury()).data
    fiscal = df[df["source"] == "fiscal_data"]
    assert set(fiscal["code"]) == {"v1/accounting/od/tcir_monthly_table_2#monthly_rate"}
    row = fiscal.iloc[0]
    assert row["data_type"] == "STRING"
    assert row["dataset"] == "Treasury Certified Interest Rates: Monthly Certification"


@respx.mock
def test_enumerate_treasury_appends_office_of_debt_management_rates() -> None:
    """The static rate-feed registry contributes catalog rows on every run,
    independent of the Fiscal Data metadata. Codes use a ``home/`` prefix to
    distinguish them from versioned Fiscal Data endpoints."""
    respx.get(_METADATA_URL).mock(return_value=httpx.Response(200, json={"datasets": []}))

    df = (enumerate_treasury()).data
    rates = df[df["source"] == "treasury_rates"]
    assert len(rates) > 30, "expected the full rate-feed registry to land"

    par_10y = rates[rates["code"] == "home/daily_treasury_yield_curve#BC_10YEAR"]
    assert not par_10y.empty
    par_10y_row = par_10y.iloc[0]
    assert par_10y_row["dataset"] == "Daily Treasury Par Yield Curve Rates"
    assert par_10y_row["title"] == "10 Year — Daily Treasury Par Yield Curve Rates"
    assert "constant-maturity" in par_10y_row["description"]
    assert par_10y_row["frequency"] == "Daily"
    assert par_10y_row["source"] == "treasury_rates"

    # endpoint is the raw feed name, passable directly to treasury_rates_fetch
    feeds = set(rates["endpoint"])
    assert {
        "daily_treasury_yield_curve",
        "daily_treasury_real_yield_curve",
        "daily_treasury_bill_rates",
        "daily_treasury_long_term_rate",
        "daily_treasury_real_long_term",
    }.issubset(feeds)


@respx.mock
def test_enumerate_treasury_maps_http_error() -> None:
    from parsimony.errors import ProviderError

    respx.get(_METADATA_URL).mock(return_value=httpx.Response(502))
    with pytest.raises(ProviderError) as exc:
        enumerate_treasury()
    assert exc.value.status_code == 502


# ---------------------------------------------------------------------------
# ODM rate-feed registry (archetype D — curated, cross-validated live by
# scripts/harvest_rate_feeds.py)
# ---------------------------------------------------------------------------


def test_rate_feed_registry_shape() -> None:
    """Pin the ODM registry shape. The registry matches the live 2025 feed column union
    exactly (cross-checked live by ``test_rate_feed_registry_has_no_live_phantom`` and
    ``scripts/harvest_rate_feeds.py``): 14 par + 5 real-curve + 14 bill + 1 long-term + 1
    real-long-term = 35. ``BC_1_5MONTH`` and the 6-week bill are real recent (2025)
    additions — present, not phantoms."""
    from parsimony_treasury.rate_feeds import build_treasury_rate_rows

    rows = build_treasury_rate_rows()
    assert len(rows) == 35
    # Every ODM code is home/<feed>#<column> with a treasury_rates source.
    assert all(r["code"].startswith("home/") and "#" in r["code"] for r in rows)
    assert {r["source"] for r in rows} == {"treasury_rates"}
    # The 2025-added maturities (1.5-month par point, 6-week bill) are catalogued.
    codes = {r["code"] for r in rows}
    assert {
        "home/daily_treasury_yield_curve#BC_10YEAR",
        "home/daily_treasury_yield_curve#BC_1_5MONTH",
        "home/daily_treasury_bill_rates#ROUND_B1_YIELD_6WK_2",
    } <= codes


def test_measure_type_prefixes_exclude_dead_rate_prefix() -> None:
    """No Fiscal Data field is typed ``RATE`` (verified live across 2,987 fields); the dead
    prefix was removed so the measure-detection set is honest."""
    from parsimony_treasury.parsing import MEASURE_TYPE_PREFIXES

    assert "RATE" not in MEASURE_TYPE_PREFIXES
    assert set(MEASURE_TYPE_PREFIXES) == {"CURRENCY", "NUMBER", "PERCENTAGE"}
