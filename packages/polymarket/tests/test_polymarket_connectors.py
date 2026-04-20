"""Happy-path tests for the Polymarket connectors.

Follows ``docs/testing-template.md``. Polymarket has no ``api_key`` dep
(public endpoints); 401/429 error-mapping tests do not apply.

Also pins the ``parsimony.transport.json_helpers.interpolate_path`` watchpoint:
this import is outside the ``docs/contract.md`` §6 public surface and the
assertion below flips CI red the day the kernel moves or renames it.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from parsimony_polymarket import (
    CONNECTORS,
    POLYMARKET_CLOB,
    POLYMARKET_GAMMA,
    PolymarketFetchParams,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"polymarket_gamma_fetch", "polymarket_clob_fetch"}


def test_polymarket_connectors_are_polymarket_tagged() -> None:
    for c in CONNECTORS:
        assert "polymarket" in c.tags


# ---------------------------------------------------------------------------
# Watchpoint: private kernel-symbol import must still resolve.
# ---------------------------------------------------------------------------


def test_json_helpers_interpolate_path_watchpoint_resolves() -> None:
    """Flip CI red the day ``parsimony.transport.json_helpers.interpolate_path`` moves.

    Polymarket is the only connector that imports this symbol today. It is NOT
    part of the public contract surface in ``parsimony/docs/contract.md`` §6
    — this test is the tripwire per the Track B council plan.
    """
    from parsimony.transport.json_helpers import interpolate_path

    rendered, remaining = interpolate_path("/markets/{id}", {"id": "foo", "limit": 10})
    assert rendered == "/markets/foo"
    assert remaining == {"limit": 10}


# ---------------------------------------------------------------------------
# polymarket_gamma
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_gamma_fetch_returns_events() -> None:
    respx.get("https://gamma-api.polymarket.com/events").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": "1234",
                    "slug": "us-election-2028",
                    "title": "US Presidential Election 2028",
                    "active": True,
                    "markets_count": 3,
                },
                {
                    "id": "5678",
                    "slug": "fed-rate-cut",
                    "title": "Fed rate cut in Q2",
                    "active": True,
                    "markets_count": 1,
                },
            ],
        )
    )

    result = await POLYMARKET_GAMMA(PolymarketFetchParams(path="/events"))

    assert result.provenance.source == "polymarket_gamma"
    df = result.data
    assert list(df["slug"]) == ["us-election-2028", "fed-rate-cut"]


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_gamma_fetch_raises_provider_error_on_500() -> None:
    from parsimony.errors import ProviderError

    respx.get("https://gamma-api.polymarket.com/events").mock(
        return_value=httpx.Response(500, text="upstream error")
    )

    with pytest.raises(ProviderError):
        await POLYMARKET_GAMMA(PolymarketFetchParams(path="/events"))


# ---------------------------------------------------------------------------
# polymarket_clob
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_polymarket_clob_fetch_supports_response_path() -> None:
    respx.get("https://clob.polymarket.com/markets").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"condition_id": "0xabc", "question": "Will X happen?"},
                    {"condition_id": "0xdef", "question": "Will Y happen?"},
                ],
                "next_cursor": None,
            },
        )
    )

    result = await POLYMARKET_CLOB(
        PolymarketFetchParams(path="/markets", response_path="data")
    )

    df = result.data
    assert result.provenance.source == "polymarket_clob"
    assert list(df["condition_id"]) == ["0xabc", "0xdef"]


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_polymarket_fetch_params_requires_path() -> None:
    with pytest.raises(ValueError):
        PolymarketFetchParams(path="")


def test_polymarket_fetch_params_allows_extra_kwargs_for_interpolation() -> None:
    p = PolymarketFetchParams(path="/markets/{id}", id="abc", limit=10)  # type: ignore[call-arg]
    assert (p.model_extra or {}).get("id") == "abc"
