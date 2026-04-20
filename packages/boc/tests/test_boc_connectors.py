"""Happy-path tests for the Bank of Canada Valet connectors.

Public API, no api_key; template 401/429 contract does not apply. BoC
constructs an httpx.AsyncClient directly (not the kernel HttpClient) so
respx still hooks into the transport.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_boc import CONNECTORS, BocFetchParams, boc_fetch


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"boc_fetch", "enumerate_boc"}


@respx.mock
@pytest.mark.asyncio
async def test_boc_fetch_single_series_returns_observations() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriesDetail": {
                    "FXUSDCAD": {"label": "USD/CAD", "description": "US dollar to Canadian dollar"}
                },
                "observations": [
                    {"d": "2026-04-17", "FXUSDCAD": {"v": "1.3852"}},
                    {"d": "2026-04-18", "FXUSDCAD": {"v": "1.3840"}},
                ],
            },
        )
    )

    result = await boc_fetch(BocFetchParams(series_name="FXUSDCAD"))

    assert result.provenance.source == "boc"
    df = result.data
    assert len(df) >= 1


@respx.mock
@pytest.mark.asyncio
async def test_boc_fetch_group_syntax_uses_group_endpoint() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriesDetail": {
                    "FXUSDCAD": {"label": "USD/CAD"},
                    "FXEURCAD": {"label": "EUR/CAD"},
                },
                "observations": [
                    {
                        "d": "2026-04-18",
                        "FXUSDCAD": {"v": "1.3840"},
                        "FXEURCAD": {"v": "1.4720"},
                    },
                ],
            },
        )
    )

    result = await boc_fetch(BocFetchParams(series_name="group:FX_RATES_DAILY"))

    assert result.provenance.source == "boc"
    assert len(result.data) >= 1


@respx.mock
@pytest.mark.asyncio
async def test_boc_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/XX/json").mock(
        return_value=httpx.Response(
            200, json={"seriesDetail": {"XX": {"label": "x"}}, "observations": []}
        )
    )

    with pytest.raises(EmptyDataError):
        await boc_fetch(BocFetchParams(series_name="XX"))


def test_fetch_rejects_empty_series_name() -> None:
    with pytest.raises(ValueError):
        BocFetchParams(series_name="   ")
