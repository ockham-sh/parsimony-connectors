"""Unit tests for the FRED connectors with mocked HTTP responses."""

from __future__ import annotations

import httpx
import pytest
import respx

from parsimony_fred import (
    CONNECTORS,
    ENV_VARS,
    FredFetchParams,
    FredSearchParams,
    enumerate_fred_release,
    fred_fetch,
    fred_search,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"fred_search", "fred_fetch"}


def test_env_vars_mapping_matches_declared_deps() -> None:
    assert ENV_VARS == {"api_key": "FRED_API_KEY"}


def test_fred_search_is_tool_tagged() -> None:
    search = next(c for c in CONNECTORS if c.name == "fred_search")
    assert "tool" in search.tags
    assert "macro" in search.tags


def test_fred_fetch_is_not_tool_tagged() -> None:
    fetch = next(c for c in CONNECTORS if c.name == "fred_fetch")
    assert "tool" not in fetch.tags


# ---------------------------------------------------------------------------
# fred_search
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fred_search_returns_series_metadata() -> None:
    respx.get("https://api.stlouisfed.org/fred/series/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriess": [
                    {
                        "id": "UNRATE",
                        "title": "Unemployment Rate",
                        "units": "Percent",
                        "frequency_short": "M",
                        "seasonal_adjustment_short": "SA",
                        "observation_start": "1948-01-01",
                        "observation_end": "2026-03-01",
                        "last_updated": "2026-04-05",
                    }
                ]
            },
        )
    )

    bound = fred_search.bind_deps(api_key="test-key")
    result = await bound(FredSearchParams(search_text="unemployment"))

    assert result.provenance.source == "fred"
    df = result.data
    assert list(df["id"]) == ["UNRATE"]


@respx.mock
@pytest.mark.asyncio
async def test_fred_search_raises_empty_data_when_no_matches() -> None:
    from parsimony.errors import EmptyDataError

    respx.get("https://api.stlouisfed.org/fred/series/search").mock(
        return_value=httpx.Response(200, json={"seriess": []})
    )

    bound = fred_search.bind_deps(api_key="test-key")
    with pytest.raises(EmptyDataError):
        await bound(FredSearchParams(search_text="nonexistent"))


# ---------------------------------------------------------------------------
# fred_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fred_fetch_returns_observations_with_metadata() -> None:
    respx.get("https://api.stlouisfed.org/fred/series/observations").mock(
        return_value=httpx.Response(
            200,
            json={
                "observations": [
                    {"date": "2020-01-01", "value": "3.5"},
                    {"date": "2020-02-01", "value": "3.6"},
                ]
            },
        )
    )
    respx.get("https://api.stlouisfed.org/fred/series").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriess": [
                    {
                        "id": "UNRATE",
                        "title": "Unemployment Rate",
                        "units": "Percent",
                        "units_short": "%",
                        "frequency": "Monthly",
                        "frequency_short": "M",
                        "seasonal_adjustment": "Seasonally Adjusted",
                        "seasonal_adjustment_short": "SA",
                        "last_updated": "2026-04-05",
                    }
                ]
            },
        )
    )

    bound = fred_fetch.bind_deps(api_key="test-key")
    result = await bound(FredFetchParams(series_id="UNRATE"))

    assert result.provenance.source == "fred"
    df = result.data
    assert "date" in df.columns
    assert "value" in df.columns
    assert list(df["series_id"]) == ["UNRATE", "UNRATE"]


# ---------------------------------------------------------------------------
# enumerate_fred_release
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_release_emits_catalog_rows() -> None:
    # Two-page response: first page full, second page empty (terminates)
    respx.get("https://api.stlouisfed.org/fred/release/series").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "seriess": [
                        {
                            "id": "GDPC1",
                            "title": "Real Gross Domestic Product",
                            "frequency_short": "Q",
                            "units_short": "Bil.",
                            "seasonal_adjustment_short": "SAAR",
                        }
                    ]
                },
            ),
            httpx.Response(200, json={"seriess": []}),
        ]
    )

    bound = enumerate_fred_release.bind_deps(api_key="test-key")
    result = await bound(release_id=53)

    df = result.data
    assert len(df) == 1
    assert df["series_id"].iloc[0] == "GDPC1"
    assert df["release_id"].iloc[0] == 53


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_fred_fetch_params_rejects_empty_series_id() -> None:
    with pytest.raises(ValueError, match="series_id"):
        FredFetchParams(series_id="   ")


def test_fred_fetch_params_accepts_date_bounds() -> None:
    p = FredFetchParams(series_id="UNRATE", observation_start="2020-01-01", observation_end="2020-12-31")
    assert p.observation_start == "2020-01-01"
    assert p.observation_end == "2020-12-31"
