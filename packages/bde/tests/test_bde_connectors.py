"""Happy-path tests for the Banco de España connectors.

BdE BIEST is public (no api_key); template 401/429 contract does not apply.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_bde import CONNECTORS, BdeFetchParams, bde_fetch


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bde_fetch", "enumerate_bde"}


@respx.mock
@pytest.mark.asyncio
async def test_bde_fetch_merges_single_series_response() -> None:
    respx.get("https://app.bde.es/bierest/resources/srdatosapp/listaSeries").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "serie": "D_1NBAF472",
                    "descripcionCorta": "Price index",
                    "codFrecuencia": "M",
                    "fechas": ["2026-01", "2026-02"],
                    "valores": ["108.4", "108.7"],
                }
            ],
        )
    )

    result = await bde_fetch(BdeFetchParams(key="D_1NBAF472"))

    assert result.provenance.source == "bde"
    df = result.data
    assert len(df) >= 1


@respx.mock
@pytest.mark.asyncio
async def test_bde_fetch_raises_empty_data_on_empty_list() -> None:
    respx.get("https://app.bde.es/bierest/resources/srdatosapp/listaSeries").mock(
        return_value=httpx.Response(200, json=[])
    )

    with pytest.raises(EmptyDataError):
        await bde_fetch(BdeFetchParams(key="XX"))


def test_fetch_rejects_invalid_time_range() -> None:
    with pytest.raises(ValueError, match="time_range"):
        BdeFetchParams(key="X", time_range="3M")


def test_fetch_rejects_empty_key() -> None:
    with pytest.raises(ValueError):
        BdeFetchParams(key="  ")
