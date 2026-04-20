"""Happy-path tests for the FMP connectors.

FMP auth is ``?apikey=<key>`` via ``HttpClient(query_params=...)``; error-
mapping contract covered via ``fmp_search`` (a tool-tagged connector).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import PaymentRequiredError, UnauthorizedError

from parsimony_fmp import (
    CONNECTORS,
    ENV_VARS,
    FmpSearchParams,
    fmp_search,
)

_KEY = "live-looking-fmp-key"


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "FMP_API_KEY"}


def test_connectors_count() -> None:
    assert len(CONNECTORS) == 18


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


@respx.mock
@pytest.mark.asyncio
async def test_fmp_search_returns_matches() -> None:
    respx.get("https://financialmodelingprep.com/stable/search-name").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc",
                    "currency": "USD",
                    "exchangeFullName": "NASDAQ",
                    "exchange": "NASDAQ",
                }
            ],
        )
    )

    bound = fmp_search.bind_deps(api_key=_KEY)
    result = await bound(FmpSearchParams(query="apple"))

    assert result.provenance.source.startswith("fmp")
    assert result.data.iloc[0]["symbol"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_fmp_search_maps_401_without_leaking_key() -> None:
    respx.get("https://financialmodelingprep.com/stable/search-name").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )

    bound = fmp_search.bind_deps(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(FmpSearchParams(query="x"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_fmp_search_maps_402_to_payment_required() -> None:
    respx.get("https://financialmodelingprep.com/stable/search-name").mock(
        return_value=httpx.Response(402, text="plan upgrade required")
    )

    bound = fmp_search.bind_deps(api_key=_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        await bound(FmpSearchParams(query="x"))
    assert _KEY not in str(exc_info.value)
