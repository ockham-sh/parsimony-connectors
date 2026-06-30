"""Integration tests for the World Bank connector — hit the live API.

These tests are marked ``@pytest.mark.integration`` and are skipped by default.
Run them explicitly with::

    uv run pytest packages/worldbank/tests -m integration -v
"""

from __future__ import annotations

import pandas as pd
import pytest

from parsimony.errors import EmptyDataError
from parsimony_worldbank.connectors.fetch import worldbank_fetch
from parsimony_worldbank.connectors.search import worldbank_search
from parsimony_worldbank.outputs import FETCH_COLUMNS, SEARCH_COLUMNS

# ===================================================================
# smoke helpers
# ===================================================================


def _check_fetch_shape(result, min_rows: int = 1) -> pd.DataFrame:
    """Assert Result wrapper has a DataFrame with expected columns and at least ``min_rows``.

    Returns the DataFrame for further assertions.
    """
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(FETCH_COLUMNS)
    assert len(df) >= min_rows
    return df


def _check_search_shape(result, min_rows: int = 1) -> pd.DataFrame:
    """Assert search Result has expected columns and at least ``min_rows``.

    Returns the DataFrame for further assertions.
    """
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(SEARCH_COLUMNS)
    assert len(df) >= min_rows
    return df


# ===================================================================
# worldbank_fetch — live API
# ===================================================================


@pytest.mark.integration
def test_integration_fetch_gdp_brazil() -> None:
    """Fetch GDP for Brazil and verify DataFrame structure."""
    result = worldbank_fetch("NY.GDP.MKTP.CD", country="BR")
    df = _check_fetch_shape(result, min_rows=1)

    # All rows should be Brazil
    assert (df["country"] == "Brazil").all()
    assert (df["country_iso3"] == "BRA").all()

    # indicator_id and indicator_name should be populated
    assert (df["indicator_id"] == "NY.GDP.MKTP.CD").all()
    assert (df["indicator_name"].str.len() > 0).all()

    # Value should be a number for some year (not every year may have data,
    # but at least one row should have a non-null value)
    assert df["value"].notna().any(), "Expected at least one non-null GDP value"
    assert pd.api.types.is_numeric_dtype(df["value"])

    # Date should be parseable as a year
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


@pytest.mark.integration
def test_integration_fetch_with_date_range() -> None:
    """Fetch GDP for Brazil restricted to 2020–2022."""
    result = worldbank_fetch("NY.GDP.MKTP.CD", country="BR", date_from="2020", date_to="2022")
    df = _check_fetch_shape(result, min_rows=1)

    # Every row's year should be >= 2020 and <= 2022
    years = df["date"].dt.year
    assert (years >= 2020).all(), f"Found year before 2020: {sorted(years.unique())}"
    assert (years <= 2022).all(), f"Found year after 2022: {sorted(years.unique())}"


@pytest.mark.integration
def test_integration_fetch_null_values() -> None:
    """Fetch GDP for all countries in 2020 — some microstates have null values.

    Not every country reports GDP every year, so the API returns ``null``
    for some country-year combinations. Those should become ``NaN`` in the
    DataFrame.
    """
    result = worldbank_fetch("NY.GDP.MKTP.CD", country="all", date_from="2020", date_to="2020")
    df = _check_fetch_shape(result, min_rows=1)

    # At least one value should be NaN (some countries don't have GDP data for 2020)
    assert df["value"].isna().any(), "Expected at least one NaN value for GDP in 2020"

    # Numeric type even with nulls
    assert pd.api.types.is_numeric_dtype(df["value"])

    # Rows with non-null values should be valid positive numbers
    valid = df["value"].dropna()
    if len(valid) > 0:
        assert (valid > 0).all(), "GDP values should be positive"


# ===================================================================
# worldbank_search — live API
# ===================================================================


@pytest.mark.integration
def test_integration_search_gdp() -> None:
    """Search for ``gdp`` — expect at least one result with ``gdp`` in name (case-insensitive)."""
    result = worldbank_search("gdp")
    df = _check_search_shape(result, min_rows=1)

    # indicator_name must be non-empty for every row
    assert (df["indicator_name"].str.len() > 0).all()
    assert (df["indicator_id"].str.len() > 0).all()

    # At least one indicator name should contain "gdp" (case-insensitive)
    has_gdp = df["indicator_name"].str.lower().str.contains("gdp")
    assert has_gdp.any(), "Expected at least one indicator with 'gdp' in the name"

    # source_id and source_name should be populated
    assert (df["source_id"].str.len() > 0).all()
    assert (df["source_name"].str.len() > 0).all()


@pytest.mark.integration
def test_integration_search_with_topic() -> None:
    """Search ``gdp`` with topic_id=3 (Economy & Growth) — expect at least one result."""
    result = worldbank_search("gdp", topic_id=3)
    df = _check_search_shape(result, min_rows=1)

    # All results should have "gdp" in the name (case-insensitive)
    has_gdp = df["indicator_name"].str.lower().str.contains("gdp")
    assert has_gdp.any(), "Expected at least one indicator with 'gdp' in the name"

    # topic_ids column should be populated (non-empty)
    assert (df["topic_ids"].str.len() > 0).all()

    # topic_ids should contain "3" (the topic we searched)
    has_topic_3 = df["topic_ids"].str.contains("3")
    assert has_topic_3.any(), "Expected at least one indicator with topic_ids containing '3'"


@pytest.mark.integration
def test_integration_search_no_results() -> None:
    """Search for a nonsense keyword — expect ``EmptyDataError``."""
    with pytest.raises(EmptyDataError, match="No indicators match"):
        worldbank_search("xyznonexistent12345")


@pytest.mark.integration
def test_integration_fetch_invalid_country() -> None:
    """Fetch with an invalid country code — expect ``EmptyDataError`` or ``InvalidParameterError``."""
    from parsimony.errors import InvalidParameterError

    try:
        worldbank_fetch("NY.GDP.MKTP.CD", country="ZZ")
    except (EmptyDataError, InvalidParameterError):
        pass
    else:
        pytest.fail("Expected EmptyDataError or InvalidParameterError for invalid country code 'ZZ'")
