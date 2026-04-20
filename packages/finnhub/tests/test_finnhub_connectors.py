"""Happy-path tests for the Finnhub connectors.

Finnhub auth is the ``X-Finnhub-Token`` header; error-mapping contract
covered via ``finnhub_search`` (tool-tagged).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import RateLimitError, UnauthorizedError

from parsimony_finnhub import (
    CONNECTORS,
    ENV_VARS,
    FinnhubQuoteParams,
    FinnhubSearchParams,
    finnhub_quote,
    finnhub_search,
)

_KEY = "live-looking-finnhub-key"


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "FINNHUB_API_KEY"}


def test_connectors_count() -> None:
    assert len(CONNECTORS) == 12


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_search_returns_matches() -> None:
    respx.get("https://finnhub.io/api/v1/search").mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "result": [
                    {
                        "description": "APPLE INC",
                        "displaySymbol": "AAPL",
                        "symbol": "AAPL",
                        "type": "Common Stock",
                    }
                ],
            },
        )
    )

    bound = finnhub_search.bind_deps(api_key=_KEY)
    result = await bound(FinnhubSearchParams(query="apple"))

    assert result.provenance.source.startswith("finnhub")
    assert result.data.iloc[0]["symbol"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_search_maps_401_without_leaking_key() -> None:
    respx.get("https://finnhub.io/api/v1/search").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )

    bound = finnhub_search.bind_deps(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(FinnhubSearchParams(query="x"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_search_maps_429_without_leaking_key() -> None:
    respx.get("https://finnhub.io/api/v1/search").mock(
        return_value=httpx.Response(429, text="rate limited")
    )

    bound = finnhub_search.bind_deps(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(FinnhubSearchParams(query="x"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_finnhub_quote_returns_single_row() -> None:
    respx.get("https://finnhub.io/api/v1/quote").mock(
        return_value=httpx.Response(
            200,
            json={"c": 171.5, "h": 172.0, "l": 169.0, "o": 170.0, "pc": 170.5, "t": 1_700_000_000},
        )
    )

    bound = finnhub_quote.bind_deps(api_key=_KEY)
    result = await bound(FinnhubQuoteParams(symbol="AAPL"))

    df = result.data
    assert len(df) == 1
