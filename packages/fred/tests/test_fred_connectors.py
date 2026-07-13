"""Unit tests for the FRED connectors with mocked HTTP responses."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import InvalidParameterError, UnauthorizedError

from parsimony_fred import (
    CONNECTORS,
    fred_fetch,
    fred_search,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"fred_search", "fred_fetch"}


def test_fred_search_is_tool_tagged() -> None:
    search = next(c for c in CONNECTORS if c.name == "fred_search")
    assert "tool" in search.tags
    assert "macro" in search.tags


def test_fred_fetch_is_not_tool_tagged() -> None:
    fetch = next(c for c in CONNECTORS if c.name == "fred_fetch")
    assert "tool" not in fetch.tags


def test_load_binds_api_key_and_hides_from_exposed_signature() -> None:
    from parsimony_fred import load

    runtime = load(api_key="secret-key")
    search = runtime["fred_search"]
    assert "api_key" not in search.exposed_signature.parameters
    assert search.bound_arguments.get("api_key") == "secret-key"


# ---------------------------------------------------------------------------
# fred_search
# ---------------------------------------------------------------------------


@respx.mock
def test_fred_search_returns_series_metadata() -> None:
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

    bound = fred_search.bind(api_key="test-key")
    result = bound(search_text="unemployment")

    assert result.provenance.source == "fred_search"
    df = result.raw
    assert list(df["id"]) == ["UNRATE"]


@respx.mock
def test_fred_search_raises_empty_data_when_no_matches() -> None:
    from parsimony.errors import EmptyDataError

    respx.get("https://api.stlouisfed.org/fred/series/search").mock(
        return_value=httpx.Response(200, json={"seriess": []})
    )

    bound = fred_search.bind(api_key="test-key")
    with pytest.raises(EmptyDataError):
        bound(search_text="nonexistent")


# ---------------------------------------------------------------------------
# fred_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_fred_fetch_raises_empty_data_when_no_observations() -> None:
    from parsimony.errors import EmptyDataError

    respx.get("https://api.stlouisfed.org/fred/series/observations").mock(
        return_value=httpx.Response(200, json={"observations": []})
    )

    bound = fred_fetch.bind(api_key="test-key")
    with pytest.raises(EmptyDataError):
        bound(series_id="UNRATE")


@respx.mock
def test_fred_fetch_raises_parse_error_when_observations_key_missing() -> None:
    from parsimony.errors import ParseError

    respx.get("https://api.stlouisfed.org/fred/series/observations").mock(
        return_value=httpx.Response(200, json={"unexpected": "shape"})
    )

    bound = fred_fetch.bind(api_key="test-key")
    with pytest.raises(ParseError):
        bound(series_id="UNRATE")


@respx.mock
def test_fred_fetch_returns_observations_with_metadata() -> None:
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

    bound = fred_fetch.bind(api_key="test-key")
    result = bound(series_id="UNRATE")

    assert result.provenance.source == "fred_fetch"
    df = result.raw
    assert "date" in df.columns
    assert "value" in df.columns
    assert list(df["series_id"]) == ["UNRATE", "UNRATE"]


# ---------------------------------------------------------------------------
# Parameter validation (inline — no separate param model)
# ---------------------------------------------------------------------------


def test_fred_fetch_rejects_empty_series_id() -> None:
    bound = fred_fetch.bind(api_key="test-key")
    with pytest.raises(InvalidParameterError, match="series_id"):
        bound(series_id="   ")


def test_fred_search_rejects_empty_search_text() -> None:
    bound = fred_search.bind(api_key="test-key")
    with pytest.raises(InvalidParameterError, match="search_text"):
        bound(search_text="   ")


def test_fred_fetch_raises_unauthorized_when_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # No bound key and no env fallback → fail fast with the env-var hint, no network.
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        fred_fetch(series_id="UNRATE")
    assert exc_info.value.env_var == "FRED_API_KEY"
    assert exc_info.value.provider == "fred"


def test_fred_search_raises_unauthorized_when_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Both verbs share _client(); assert the symmetric no-key fast-fail for search too.
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        fred_search(search_text="unemployment")
    assert exc_info.value.env_var == "FRED_API_KEY"
    assert exc_info.value.provider == "fred"
