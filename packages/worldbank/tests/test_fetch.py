"""Unit tests for ``worldbank_fetch`` with mocked HTTP responses."""

from __future__ import annotations

import pandas as pd
import pytest
import respx

from parsimony.errors import EmptyDataError, InvalidParameterError
from parsimony_worldbank import CONNECTORS
from parsimony_worldbank.connectors.fetch import worldbank_fetch
from parsimony_worldbank.outputs import FETCH_COLUMNS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_URL = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD"


def _make_response_page(
    records: list[dict],
    page: int = 1,
    per_page: int = 100,
    total: int | None = None,
) -> list:
    """Build a World Bank API response envelope."""
    if total is None:
        total = len(records)
    pages = max(1, -(-total // per_page))  # ceil division
    return [
        {"page": page, "pages": pages, "per_page": per_page, "total": total},
        records,
    ]


# ---------------------------------------------------------------------------
# Successful fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_success() -> None:
    """``worldbank_fetch`` returns a DataFrame with correct columns."""
    respx.get(MOCK_URL).respond(
        200,
        json=_make_response_page(
            [
                {
                    "indicator": {
                        "id": "NY.GDP.MKTP.CD",
                        "value": "GDP (current US$)",
                    },
                    "country": {"id": "BR", "value": "Brazil"},
                    "countryiso3code": "BRA",
                    "date": "2024",
                    "value": 2185821648943.86,
                },
                {
                    "indicator": {
                        "id": "NY.GDP.MKTP.CD",
                        "value": "GDP (current US$)",
                    },
                    "country": {"id": "US", "value": "United States"},
                    "countryiso3code": "USA",
                    "date": "2023",
                    "value": 27364873500000.0,
                },
            ],
        ),
    )

    result = worldbank_fetch(indicator_id="NY.GDP.MKTP.CD")
    df = result.data

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(FETCH_COLUMNS)
    assert len(df) == 2
    assert df.iloc[0]["country"] == "Brazil"
    assert df.iloc[0]["value"] == 2185821648943.86
    assert df.iloc[1]["country_iso3"] == "USA"
    assert df.iloc[1]["date"].year == 2023


# ---------------------------------------------------------------------------
# Null values
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_null_values() -> None:
    """``value: null`` in the API response becomes ``NaN`` in the DataFrame."""
    respx.get(MOCK_URL).respond(
        200,
        json=_make_response_page(
            [
                {
                    "indicator": {
                        "id": "NY.GDP.MKTP.CD",
                        "value": "GDP (current US$)",
                    },
                    "country": {"id": "BR", "value": "Brazil"},
                    "countryiso3code": "BRA",
                    "date": "2024",
                    "value": None,  # null in JSON
                },
                {
                    "indicator": {
                        "id": "NY.GDP.MKTP.CD",
                        "value": "GDP (current US$)",
                    },
                    "country": {"id": "US", "value": "United States"},
                    "countryiso3code": "USA",
                    "date": "2023",
                    "value": 100.0,
                },
            ],
        ),
    )

    result = worldbank_fetch(indicator_id="NY.GDP.MKTP.CD")
    df = result.data

    assert len(df) == 2
    assert pd.isna(df.iloc[0]["value"]), "Expected NaN for null API value"
    assert df.iloc[1]["value"] == 100.0


# ---------------------------------------------------------------------------
# Empty indicator_id
# ---------------------------------------------------------------------------


def test_fetch_empty_indicator_raises_invalid_parameter() -> None:
    """Empty ``indicator_id`` raises ``InvalidParameterError``."""
    with pytest.raises(InvalidParameterError, match="indicator_id must be non-empty"):
        worldbank_fetch(indicator_id="   ")


def test_fetch_empty_indicator_strict() -> None:
    """Empty string indicator_id (no whitespace) also raises."""
    with pytest.raises(InvalidParameterError, match="indicator_id must be non-empty"):
        worldbank_fetch(indicator_id="")


# ---------------------------------------------------------------------------
# Empty data (API returns no records)
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_empty_data_raises_empty_data_error() -> None:
    """API returning no data raises ``EmptyDataError``."""
    respx.get(MOCK_URL).respond(
        200,
        json=_make_response_page([], total=0),
    )

    with pytest.raises(EmptyDataError, match="No data for indicator=NY.GDP.MKTP.CD"):
        worldbank_fetch(indicator_id="NY.GDP.MKTP.CD")


# ---------------------------------------------------------------------------
# Via CONNECTORS collection
# ---------------------------------------------------------------------------


def test_fetch_is_in_connectors() -> None:
    """The connector is exposed in the ``CONNECTORS`` collection."""
    names = {c.name for c in CONNECTORS}
    assert "worldbank_fetch" in names
