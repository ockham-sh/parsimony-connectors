"""Happy-path tests for the Destatis connectors.

Destatis GENESIS uses ``username`` + ``password`` credentials (not ``api_key``),
passed as query params. The ``docs/testing-template.md`` §4 401/429 contract
targets ``api_key`` / ``token`` deps only — skipped here. The happy-path
fixture returns a minimal ffcsv response and asserts the kernel Result shape.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import ProviderError

from parsimony_destatis import (
    CONNECTORS,
    ENV_VARS,
    DestatisFetchParams,
    destatis_fetch,
)

_FFCSV_FIXTURE = (
    "Statistik_Code;Statistik_Label;Zeit;Wert\n"
    "61111-0001;Consumer Prices;2026 Januar;108.4\n"
    "61111-0001;Consumer Prices;2026 Februar;108.7\n"
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_env_vars_maps_username_and_password() -> None:
    assert ENV_VARS == {
        "username": "DESTATIS_USERNAME",
        "password": "DESTATIS_PASSWORD",
    }


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"destatis_fetch", "enumerate_destatis"}


# ---------------------------------------------------------------------------
# destatis_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_parses_ffcsv_response() -> None:
    respx.get(
        "https://www-genesis.destatis.de/genesisWS/rest/2020/data/tablefile"
    ).mock(return_value=httpx.Response(200, text=_FFCSV_FIXTURE))

    result = await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))

    assert result.provenance.source == "destatis"
    df = result.data
    assert len(df) == 2
    assert "table_id" in df.columns
    assert df.iloc[0]["table_id"] == "61111-0001"


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_maps_500_to_provider_error() -> None:
    respx.get(
        "https://www-genesis.destatis.de/genesisWS/rest/2020/data/tablefile"
    ).mock(return_value=httpx.Response(500, text="upstream error"))

    with pytest.raises(ProviderError):
        await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_raises_provider_error_on_announcement_redirect() -> None:
    respx.get(
        "https://www-genesis.destatis.de/genesisWS/rest/2020/data/tablefile"
    ).mock(
        return_value=httpx.Response(
            200, text="<html><body>Wartungsarbeiten announcement</body></html>"
        )
    )

    with pytest.raises(ProviderError, match="announcement"):
        await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_fetch_requires_table_id() -> None:
    with pytest.raises(ValueError):
        DestatisFetchParams(table_id="")
