"""Error-mapping tests for the World Bank connectors.

The World Bank API is keyless, so there is no ``UnauthorizedError`` path.
Tests cover the remaining HTTP status → parsimony error mapping.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, ParseError, ProviderError

from parsimony_worldbank import worldbank_fetch, worldbank_search

_BASE = "https://api.worldbank.org/v2"


# ---------------------------------------------------------------------------
# ProviderError mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "op_name"),
    [
        (400, "country/all/indicator/SP.POP.TOTL"),
        (404, "country/all/indicator/SP.POP.TOTL"),
        (500, "country/all/indicator/SP.POP.TOTL"),
        (502, "country/all/indicator/SP.POP.TOTL"),
        (503, "country/all/indicator/SP.POP.TOTL"),
    ],
)
@respx.mock
def test_worldbank_fetch_maps_http_status_to_provider_error(status: int, op_name: str) -> None:
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(status)
    )

    with pytest.raises(ProviderError) as exc:
        worldbank_fetch(indicator="SP.POP.TOTL")
    assert exc.value.status_code == status
    assert exc.value.provider == "worldbank"


@pytest.mark.parametrize("status", [400, 404, 500, 502, 503])
@respx.mock
def test_worldbank_search_maps_http_status_to_provider_error(status: int) -> None:
    respx.get(f"{_BASE}/indicator", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(status)
    )

    with pytest.raises(ProviderError) as exc:
        worldbank_search(query="test")
    assert exc.value.status_code == status
    assert exc.value.provider == "worldbank"


# ---------------------------------------------------------------------------
# ParseError — malformed response
# ---------------------------------------------------------------------------


@respx.mock
def test_worldbank_fetch_raises_parse_error_on_non_array_body() -> None:
    """If the API returns a plain object (not a two-element array), raise
    ParseError."""
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(200, json={"error": "not the expected format"})
    )

    with pytest.raises(ParseError):
        worldbank_fetch(indicator="SP.POP.TOTL")


@respx.mock
def test_worldbank_fetch_raises_parse_error_on_null_data() -> None:
    """If the API returns [metadata, null] instead of [metadata, [...]],
    raise a clear error."""
    respx.get(f"{_BASE}/country/all/indicator/SP.POP.TOTL", params={"format": "json", "per_page": 50000}).mock(
        return_value=httpx.Response(200, json=[{"page": 1}, None])
    )

    with pytest.raises(EmptyDataError):
        worldbank_fetch(indicator="SP.POP.TOTL")

