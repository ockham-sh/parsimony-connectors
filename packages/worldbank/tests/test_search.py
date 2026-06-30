"""Unit tests for ``worldbank_search`` with mocked HTTP responses."""

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

MOCK_SEARCH_URL = "https://api.worldbank.org/v2/indicator"


def _make_search_response(
    records: list[dict],
    page: int = 1,
    per_page: int = 50,
    total: int | None = None,
) -> list:
    """Build a World Bank search API response envelope."""
    if total is None:
        total = len(records)
    pages = max(1, -(-total // per_page))  # ceil division
    return [
        {"page": page, "pages": pages, "per_page": per_page, "total": total},
        records,
    ]


# ---------------------------------------------------------------------------
# Successful search
# ---------------------------------------------------------------------------


@respx.mock
def test_search_success() -> None:
    """``worldbank_search`` returns a DataFrame with correct columns and values."""
    respx.get(MOCK_SEARCH_URL).respond(
        200,
        json=_make_search_response(
            [
                {
                    "id": "NY.GDP.MKTP.CD",
                    "name": "GDP (current US$)",
                    "sourceNote": "GDP at purchaser's prices is the sum of gross value added ...",
                    "sourceOrganization": "World Bank",
                    "sourceId": "2",
                    "unit": "",
                    "decimal": 0,
                },
                {
                    "id": "NY.GDP.MKTP.KD.ZG",
                    "name": "GDP growth (annual %)",
                    "sourceNote": "Annual percentage growth rate of GDP at market prices ...",
                    "sourceOrganization": "World Bank",
                    "sourceId": "2",
                    "unit": "",
                    "decimal": 1,
                },
            ],
            total=2,
        ),
    )

    result = worldbank_search(query="GDP")
    df = result.data

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(SEARCH_COLUMNS)
    assert len(df) == 2

    # First row
    assert df.iloc[0]["indicator_id"] == "NY.GDP.MKTP.CD"
    assert df.iloc[0]["indicator_name"] == "GDP (current US$)"
    assert "GDP at purchaser's prices" in df.iloc[0]["source_note"]
    assert df.iloc[0]["source_org"] == "World Bank"
    assert df.iloc[0]["page"] == 1

    # Second row
    assert df.iloc[1]["indicator_id"] == "NY.GDP.MKTP.KD.ZG"
    assert df.iloc[1]["indicator_name"] == "GDP growth (annual %)"


# ---------------------------------------------------------------------------
# No results
# ---------------------------------------------------------------------------


@respx.mock
def test_search_no_results() -> None:
    """API returning no matches raises ``EmptyDataError``."""
    respx.get(MOCK_SEARCH_URL).respond(
        200,
        json=_make_search_response([], total=0),
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
    respx.get(MOCK_SEARCH_URL).respond(200, json=[])

    with pytest.raises(EmptyDataError, match=r"No indicators match query='GDP'"):
        worldbank_search(query="GDP")


@respx.mock
def test_search_api_returns_singleton() -> None:
    """API returning ``[meta]`` without records raises ``EmptyDataError``."""
    respx.get(MOCK_SEARCH_URL).respond(
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
