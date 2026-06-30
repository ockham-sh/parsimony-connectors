"""Offline (respx-mocked) tests for the World Bank connectors.

World Bank indicators API is keyless — no authentication required. All
tests use mocked HTTP responses via ``respx``.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import ColumnRole

from parsimony_worldbank import (
    CONNECTORS,
    FETCH_OUTPUT,
    SEARCH_OUTPUT,
    worldbank_fetch,
    worldbank_search,
)

_BASE = "https://api.worldbank.org/v2"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"worldbank_search", "worldbank_fetch"}


def test_worldbank_search_is_tool_tagged() -> None:
    search = next(c for c in CONNECTORS if c.name == "worldbank_search")
    assert "tool" in search.tags
    assert "macro" in search.tags
    assert "worldbank" in search.tags


def test_worldbank_fetch_is_not_tool_tagged() -> None:
    fetch = next(c for c in CONNECTORS if c.name == "worldbank_fetch")
    assert "tool" not in fetch.tags
    assert "macro" in fetch.tags
    assert "worldbank" in fetch.tags


def test_worldbank_fetch_no_secrets() -> None:
    """World Bank is keyless — no secrets declared."""
    fetch = next(c for c in CONNECTORS if c.name == "worldbank_fetch")
    assert fetch.secrets == ()


def test_worldbank_search_output_has_key_namespace() -> None:
    key_cols = [c for c in SEARCH_OUTPUT.columns if c.role == ColumnRole.KEY]
    assert len(key_cols) == 1
    assert key_cols[0].name == "id"
    assert key_cols[0].namespace == "worldbank"


def test_worldbank_fetch_output_has_key_namespace() -> None:
    key_cols = [c for c in FETCH_OUTPUT.columns if c.role == ColumnRole.KEY]
    assert len(key_cols) == 1
    assert key_cols[0].name == "indicator"
    assert key_cols[0].namespace == "worldbank"


# ---------------------------------------------------------------------------
# worldbank_search
# ---------------------------------------------------------------------------


@respx.mock
def test_worldbank_search_filters_by_keyword() -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 5, "total": 5},
                [
                    {
                        "id": "SP.POP.TOTL",
                        "name": "Population, total",
                        "sourceNote": "Total population counts all residents.",
                        "sourceOrganization": "World Bank",
                        "unit": "",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                    {
                        "id": "NY.GDP.MKTP.CD",
                        "name": "GDP (current US$)",
                        "sourceNote": "GDP at purchaser's prices.",
                        "sourceOrganization": "World Bank",
                        "unit": "Current US$",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                    {
                        "id": "SE.PRM.ENRR",
                        "name": "School enrollment, primary (% gross)",
                        "sourceNote": "Gross enrollment ratio.",
                        "sourceOrganization": "UNESCO",
                        "unit": "",
                        "source": {"id": "3", "value": "Education Statistics"},
                    },
                ],
            ],
        )
    )

    result = worldbank_search(query="population")

    df = result.data
    assert result.provenance.source == "worldbank_search"
    assert len(df) == 1
    assert list(df["id"]) == ["SP.POP.TOTL"]
    assert list(df["name"]) == ["Population, total"]


@respx.mock
def test_worldbank_search_matches_indicator_code() -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 3, "total": 3},
                [
                    {
                        "id": "SP.POP.TOTL",
                        "name": "Population, total",
                        "sourceNote": "Total population.",
                        "sourceOrganization": "World Bank",
                        "unit": "",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                    {
                        "id": "SP.DYN.LE00.IN",
                        "name": "Life expectancy at birth, total (years)",
                        "sourceNote": "Life expectancy.",
                        "sourceOrganization": "World Bank",
                        "unit": "Years",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                ],
            ],
        )
    )

    result = worldbank_search(query="SP.POP")

    df = result.data
    assert len(df) == 1
    assert list(df["id"]) == ["SP.POP.TOTL"]


@respx.mock
def test_worldbank_search_is_case_insensitive() -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 2, "total": 2},
                [
                    {
                        "id": "NY.GDP.MKTP.CD",
                        "name": "GDP (current US$)",
                        "sourceNote": "GDP at purchaser's prices.",
                        "sourceOrganization": "World Bank",
                        "unit": "Current US$",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                ],
            ],
        )
    )

    result = worldbank_search(query="gdp")

    assert len(result.data) == 1
    assert result.data["id"].iloc[0] == "NY.GDP.MKTP.CD"


@respx.mock
def test_worldbank_search_raises_empty_data_on_no_match() -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 1, "total": 1},
                [
                    {
                        "id": "SP.POP.TOTL",
                        "name": "Population, total",
                        "sourceNote": "...",
                        "sourceOrganization": "World Bank",
                        "unit": "",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                ],
            ],
        )
    )

    with pytest.raises(EmptyDataError):
        worldbank_search(query="nonexistent_xyzzy")


@respx.mock
def test_worldbank_search_raises_empty_data_on_empty_api_response() -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(200, json=[{"page": 1, "pages": 0, "total": 0}, []])
    )

    with pytest.raises(EmptyDataError):
        worldbank_search(query="anything")


def test_worldbank_search_rejects_empty_query() -> None:
    with pytest.raises(InvalidParameterError):
        worldbank_search(query="   ")


@respx.mock
def test_worldbank_search_sorts_results_by_id() -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 3, "total": 3},
                [
                    {
                        "id": "AG.LND.AGRI.ZS",
                        "name": "Agricultural land (% of land area)",
                        "sourceNote": "...",
                        "sourceOrganization": "World Bank",
                        "unit": "",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                    {
                        "id": "AG.LND.FRST.ZS",
                        "name": "Forest area (% of land area)",
                        "sourceNote": "...",
                        "sourceOrganization": "World Bank",
                        "unit": "",
                        "source": {"id": "2", "value": "World Development Indicators"},
                    },
                ],
            ],
        )
    )

    result = worldbank_search(query="land")

    df = result.data
    assert list(df["id"]) == ["AG.LND.AGRI.ZS", "AG.LND.FRST.ZS"]


# ---------------------------------------------------------------------------
# worldbank_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_worldbank_fetch_returns_records() -> None:
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 2, "total": 2},
                [
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2024",
                        "value": "339996563",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "IN", "value": "India"},
                        "countryiso3code": "IND",
                        "date": "2024",
                        "value": "1450935791",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                ],
            ],
        )
    )

    result = worldbank_fetch(indicator="SP.POP.TOTL")

    assert result.provenance.source == "worldbank_fetch"
    df = result.data
    assert len(df) == 2
    assert list(df["indicator"]) == ["SP.POP.TOTL", "SP.POP.TOTL"]
    assert list(df["country"]) == ["United States", "India"]
    assert list(df["countryiso3code"]) == ["USA", "IND"]
    # Values are coerced to numeric (float).
    assert df["value"].iloc[0] == 339996563.0
    assert df["value"].iloc[1] == 1450935791.0


@respx.mock
def test_worldbank_fetch_with_single_country() -> None:
    respx.get(f"{_BASE}/country/US/indicator/NY.GDP.MKTP.CD", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 1, "total": 1},
                [
                    {
                        "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP (current US$)"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2024",
                        "value": "28781000000000",
                        "unit": "Current US$",
                        "obs_status": "",
                        "decimal": 0,
                    },
                ],
            ],
        )
    )

    result = worldbank_fetch(indicator="NY.GDP.MKTP.CD", country="US")

    df = result.data
    assert len(df) == 1
    assert list(df["country"]) == ["United States"]
    assert list(df["unit"]) == ["Current US$"]


@respx.mock
def test_worldbank_fetch_with_date_range() -> None:
    route = respx.get(
        f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000, "date": "2020:2024"}
    ).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 2, "total": 2},
                [
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2024",
                        "value": "339996563",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2020",
                        "value": "331500000",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                ],
            ],
        )
    )

    result = worldbank_fetch(indicator="SP.POP.TOTL", date="2020:2024")

    assert route.called
    df = result.data
    assert len(df) == 2
    assert set(df["date"]) == {pd.Timestamp("2024-01-01"), pd.Timestamp("2020-01-01")}


@respx.mock
def test_worldbank_fetch_handles_null_values() -> None:
    """The API returns null for missing observations — these become NaN."""
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 2, "total": 2},
                [
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2024",
                        "value": "339996563",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2023",
                        "value": None,
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                ],
            ],
        )
    )

    df = (worldbank_fetch(indicator="SP.POP.TOTL")).data
    assert len(df) == 2
    assert pd.isna(df["value"].iloc[1])


@respx.mock
def test_worldbank_fetch_raises_empty_data_on_empty_response() -> None:
    respx.get(f"{_BASE}/country/all/indicator/NONEXISTENT", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(200, json=[{"page": 1, "pages": 0, "total": 0}, []])
    )

    with pytest.raises(EmptyDataError):
        worldbank_fetch(indicator="NONEXISTENT")


@respx.mock
def test_worldbank_fetch_raises_parse_error_on_single_message() -> None:
    """Some endpoints return a message array on HTTP 200 for invalid
    requests, e.g. ``[{"message": "Invalid format"}]``."""
    respx.get(f"{_BASE}/country/INVALID/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[{"page": 1, "pages": 1, "per_page": 1, "total": 1}, [{"message": "Invalid format"}]],
        )
    )

    with pytest.raises(ParseError):
        worldbank_fetch(indicator="SP.POP.TOTL", country="INVALID")


@respx.mock
def test_worldbank_fetch_maps_http_error() -> None:
    from parsimony.errors import ProviderError

    respx.get(f"{_BASE}/country/all/indicator/BAD", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(503)
    )

    with pytest.raises(ProviderError) as exc:
        worldbank_fetch(indicator="BAD")
    assert exc.value.status_code == 503


@respx.mock
def test_worldbank_fetch_maps_http_404_as_provider_error() -> None:
    from parsimony.errors import ProviderError

    respx.get(f"{_BASE}/country/all/indicator/NONEXIST", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(404)
    )

    with pytest.raises(ProviderError):
        worldbank_fetch(indicator="NONEXIST")


def test_worldbank_fetch_rejects_empty_indicator() -> None:
    with pytest.raises(InvalidParameterError):
        worldbank_fetch(indicator="   ")


def test_worldbank_fetch_rejects_empty_country() -> None:
    with pytest.raises(InvalidParameterError):
        worldbank_fetch(indicator="SP.POP.TOTL", country="   ")


@respx.mock
def test_worldbank_fetch_returns_expected_columns() -> None:
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 1, "total": 1},
                [
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2024",
                        "value": "339996563",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                ],
            ],
        )
    )

    df = (worldbank_fetch(indicator="SP.POP.TOTL")).data
    assert list(df.columns) == [c.name for c in FETCH_OUTPUT.columns]


@respx.mock
def test_worldbank_fetch_contains_provenance() -> None:
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1, "pages": 1, "per_page": 1, "total": 1},
                [
                    {
                        "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
                        "country": {"id": "US", "value": "United States"},
                        "countryiso3code": "USA",
                        "date": "2024",
                        "value": "339996563",
                        "unit": "",
                        "obs_status": "",
                        "decimal": 0,
                    },
                ],
            ],
        )
    )

    result = worldbank_fetch(indicator="SP.POP.TOTL")
    assert result.provenance.source == "worldbank_fetch"
    assert result.provenance.params == {
        "country": "all",
        "indicator": "SP.POP.TOTL",
        "date": None,
    }
