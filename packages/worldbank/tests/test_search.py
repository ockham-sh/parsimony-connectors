"""Unit tests for ``worldbank_search`` with mocked HTTP responses."""

# GREP_SUMMARY: test_search — unit tests for worldbank_search with respx mocks

from __future__ import annotations

import pandas as pd
import pytest
import respx

from parsimony.errors import EmptyDataError
from parsimony_worldbank.connectors.search import worldbank_search
from parsimony_worldbank.outputs import SEARCH_COLUMNS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_SOURCE_URL = "https://api.worldbank.org/v2/source/2/indicator"
MOCK_TOPIC_URL = "https://api.worldbank.org/v2/topic/3/indicator"


def _make_search_response(
    records: list[dict],
    page: int = 1,
    per_page: int = 50,
    total: int | None = None,
) -> list:
    """Build a World Bank paginated API response envelope.

    Returns ``[meta_dict, records_list]`` matching the WB API v2 format.
    """
    if total is None:
        total = len(records)
    pages = max(1, -(-total // per_page))  # ceil division
    return [
        {"page": page, "pages": pages, "per_page": per_page, "total": total},
        records,
    ]


def _gdp_records() -> list[dict]:
    """Return two mock indicator records with GDP in their name."""
    return [
        {
            "id": "NY.GDP.MKTP.CD",
            "name": "GDP (current US$)",
            "source": {"id": "2", "value": "World Development Indicators"},
            "topics": [{"id": "3", "value": "Economic Policy & Debt"}],
        },
        {
            "id": "NY.GDP.MKTP.KD.ZG",
            "name": "GDP growth (annual %)",
            "source": {"id": "2", "value": "World Development Indicators"},
            "topics": [{"id": "3", "value": "Economic Policy & Debt"}],
        },
    ]


# ---------------------------------------------------------------------------
# Successful search — source endpoint
# ---------------------------------------------------------------------------


@respx.mock
def test_search_success() -> None:
    """``worldbank_search`` returns a DataFrame with correct columns and values."""
    respx.get(MOCK_SOURCE_URL).respond(
        200,
        json=_make_search_response(_gdp_records(), total=2),
    )

    result = worldbank_search(query="GDP")
    df = result.data

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(SEARCH_COLUMNS)
    assert len(df) == 2

    # First row
    assert df.iloc[0]["indicator_id"] == "NY.GDP.MKTP.CD"
    assert df.iloc[0]["indicator_name"] == "GDP (current US$)"
    assert df.iloc[0]["source_id"] == "2"
    assert df.iloc[0]["source_name"] == "World Development Indicators"
    assert df.iloc[0]["topic_ids"] == "3"

    # Second row
    assert df.iloc[1]["indicator_id"] == "NY.GDP.MKTP.KD.ZG"
    assert df.iloc[1]["indicator_name"] == "GDP growth (annual %)"


# ---------------------------------------------------------------------------
# Successful search — topic endpoint
# ---------------------------------------------------------------------------


@respx.mock
def test_search_with_topic_id() -> None:
    """``worldbank_search`` with ``topic_id`` uses the topic endpoint."""
    respx.get(MOCK_TOPIC_URL).respond(
        200,
        json=_make_search_response(
            [
                {
                    "id": "SP.POP.TOTL",
                    "name": "Population, total",
                    "source": {"id": "2", "value": "World Development Indicators"},
                    "topics": [{"id": "3", "value": "Economic Policy & Debt"}],
                },
            ],
            total=1,
        ),
    )

    result = worldbank_search(query="population", topic_id=3)
    df = result.data

    assert len(df) == 1
    assert df.iloc[0]["indicator_id"] == "SP.POP.TOTL"
    assert df.iloc[0]["indicator_name"] == "Population, total"


# ---------------------------------------------------------------------------
# No results (records exist but name does not match)
# ---------------------------------------------------------------------------


@respx.mock
def test_search_no_results() -> None:
    """API returning records where none match raises ``EmptyDataError``."""
    respx.get(MOCK_SOURCE_URL).respond(
        200,
        json=_make_search_response(
            [
                {
                    "id": "NY.GDP.MKTP.CD",
                    "name": "GDP (current US$)",
                    "source": {"id": "2", "value": "World Development Indicators"},
                    "topics": [{"id": "3", "value": "Economic Policy & Debt"}],
                },
            ],
            total=1,
        ),
    )

    with pytest.raises(EmptyDataError, match=r"No indicators match query='TURBOENCABULATOR'"):
        worldbank_search(query="TURBOENCABULATOR")


# ---------------------------------------------------------------------------
# Empty query
# ---------------------------------------------------------------------------


def test_search_empty_query() -> None:
    """Empty ``query`` raises ``EmptyDataError``."""
    with pytest.raises(EmptyDataError, match="Search query must be non-empty"):
        worldbank_search(query="")

    with pytest.raises(EmptyDataError, match="Search query must be non-empty"):
        worldbank_search(query="   ")


# ---------------------------------------------------------------------------
# Empty API response (malformed / truncated)
# ---------------------------------------------------------------------------


@respx.mock
def test_search_api_returns_empty_list() -> None:
    """API returning ``[]`` raises ``EmptyDataError``."""
    respx.get(MOCK_SOURCE_URL).respond(200, json=[])

    with pytest.raises(EmptyDataError, match=r"No indicators match query='GDP'"):
        worldbank_search(query="GDP")


@respx.mock
def test_search_api_returns_singleton() -> None:
    """API returning ``[meta]`` without records raises ``EmptyDataError``."""
    respx.get(MOCK_SOURCE_URL).respond(
        200,
        json=[{"page": 1, "pages": 1, "per_page": 50, "total": 0}],
    )

    with pytest.raises(EmptyDataError, match=r"No indicators match query='GDP'"):
        worldbank_search(query="GDP")


# ---------------------------------------------------------------------------
# Via CONNECTORS collection
# ---------------------------------------------------------------------------


def test_search_is_in_connectors() -> None:
    """The connector is exposed in the ``CONNECTORS`` collection."""
    from parsimony_worldbank import CONNECTORS

    names = {c.name for c in CONNECTORS}
    assert "worldbank_search" in names
