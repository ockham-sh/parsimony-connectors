"""Happy-path tests for the EIA connectors.

Follows ``CONTRIBUTING.md §4``. EIA auth is ``?api_key=<key>`` via
``HttpClient(query_params=...)``; HTTP errors are mapped to
``parsimony.errors`` (``UnauthorizedError`` / ``RateLimitError`` /
``EmptyDataError``) at the transport boundary.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, RateLimitError, UnauthorizedError

from parsimony_eia import (
    CONNECTORS,
    eia_fetch,
    enumerate_eia,
)

_KEY = "live-looking-eia-xyz"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"eia_fetch", "enumerate_eia"}


# ---------------------------------------------------------------------------
# eia_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_eia_fetch_returns_rows() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "description": "Spot Prices",
                    "data": [
                        {"period": "2026-03", "value": 78.5, "duoarea": "NUS", "product": "EPCBRENT"},
                        {"period": "2026-02", "value": 77.0, "duoarea": "NUS", "product": "EPCBRENT"},
                    ],
                }
            },
        )
    )

    bound = eia_fetch.bind(api_key=_KEY)
    result = bound(route="petroleum/pri/spt")

    assert result.provenance.source == "eia_fetch"
    assert len(result.data) == 2
    assert result.data.iloc[0]["title"] == "Spot Prices"


@respx.mock
def test_eia_fetch_maps_401_without_leaking_key() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(401, text="invalid api key")
    )

    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        bound(route="petroleum/pri/spt")
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_eia_fetch_maps_429_without_leaking_key() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "30"}, text="too many")
    )

    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        bound(route="petroleum/pri/spt")
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_eia_fetch_raises_empty_data_when_no_records() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/data").mock(
        return_value=httpx.Response(200, json={"response": {"data": []}})
    )

    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(EmptyDataError):
        bound(route="petroleum/pri/spt")


# ---------------------------------------------------------------------------
# enumerate_eia
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_eia_returns_routes() -> None:
    respx.get("https://api.eia.gov/v2/").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "routes": [
                        {"id": "petroleum", "name": "Petroleum", "description": "Crude oil and products"},
                        {"id": "natural-gas", "name": "Natural Gas", "description": "Natural gas data"},
                    ]
                }
            },
        )
    )

    bound = enumerate_eia.bind(api_key=_KEY)
    result = bound()

    df = result.data
    assert list(df.columns) == ["route", "title", "description"]
    assert set(df["route"]) == {"petroleum", "natural-gas"}
    assert set(df["description"]) == {"Crude oil and products", "Natural gas data"}


# ---------------------------------------------------------------------------
# Parameter validation (inline — no separate param model)
# ---------------------------------------------------------------------------


def test_eia_fetch_rejects_empty_route() -> None:
    bound = eia_fetch.bind(api_key=_KEY)
    with pytest.raises(InvalidParameterError, match="route"):
        bound(route="   ")


def test_eia_fetch_raises_unauthorized_when_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        eia_fetch(route="petroleum/pri/spt")
    assert exc_info.value.env_var == "EIA_API_KEY"
    assert exc_info.value.provider == "eia"


def test_enumerate_eia_raises_unauthorized_when_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Both verbs share _client(); assert the symmetric no-key fast-fail for the enumerator too.
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        enumerate_eia()
    assert exc_info.value.env_var == "EIA_API_KEY"
    assert exc_info.value.provider == "eia"
