"""Happy-path tests for the Tiingo connectors.

Follows ``docs/testing-template.md``. Tiingo auth is a ``Authorization: Token
<key>`` header; error-mapping contract covered on ``tiingo_search``.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import RateLimitError, UnauthorizedError

from parsimony_tiingo import (
    CONNECTORS,
    ENV_VARS,
    TiingoEodParams,
    TiingoSearchParams,
    tiingo_eod,
    tiingo_search,
)

_KEY = "live-looking-tiingo-xyz"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "TIINGO_API_KEY"}


def test_connectors_count_matches_docstring() -> None:
    assert len(CONNECTORS) == 13


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


# ---------------------------------------------------------------------------
# tiingo_search (tool-tagged, error-mapping contract)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_tiingo_search_returns_rows() -> None:
    respx.get("https://api.tiingo.com/tiingo/utilities/search").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "ticker": "AAPL",
                    "name": "Apple Inc",
                    "assetType": "Stock",
                    "exchange": "NASDAQ",
                    "countryCode": "USA",
                }
            ],
        )
    )

    bound = tiingo_search.bind_deps(api_key=_KEY)
    result = await bound(TiingoSearchParams(query="apple"))

    assert result.provenance.source == "tiingo_search"
    assert result.data.iloc[0]["ticker"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_tiingo_search_maps_401_without_leaking_key() -> None:
    respx.get("https://api.tiingo.com/tiingo/utilities/search").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )

    bound = tiingo_search.bind_deps(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(TiingoSearchParams(query="apple"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_tiingo_search_maps_429_without_leaking_key() -> None:
    respx.get("https://api.tiingo.com/tiingo/utilities/search").mock(
        return_value=httpx.Response(429, text="rate limited")
    )

    bound = tiingo_search.bind_deps(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(TiingoSearchParams(query="apple"))
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# tiingo_eod
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_tiingo_eod_returns_ohlcv() -> None:
    respx.get("https://api.tiingo.com/tiingo/daily/AAPL/prices").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2026-04-18T00:00:00.000Z",
                    "open": 170.0,
                    "high": 172.0,
                    "low": 169.0,
                    "close": 171.5,
                    "volume": 45000000,
                    "adjClose": 171.5,
                    "adjOpen": 170.0,
                    "adjHigh": 172.0,
                    "adjLow": 169.0,
                    "adjVolume": 45000000,
                    "divCash": 0,
                    "splitFactor": 1,
                }
            ],
        )
    )

    bound = tiingo_eod.bind_deps(api_key=_KEY)
    result = await bound(TiingoEodParams(ticker="AAPL"))

    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["close"] == 171.5


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_eod_rejects_unsafe_ticker() -> None:
    with pytest.raises(ValueError):
        TiingoEodParams(ticker="../etc/passwd")
