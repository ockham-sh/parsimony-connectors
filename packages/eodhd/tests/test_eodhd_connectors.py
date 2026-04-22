"""Happy-path tests for the EODHD connectors.

Follows ``docs/testing-template.md``. EODHD auth is ``?api_token=<key>``
via ``HttpClient(query_params=...)``; error-mapping contract covered on
``eodhd_search`` which is tool-tagged.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import RateLimitError, UnauthorizedError

from parsimony_eodhd import (
    CONNECTORS,
    EodhdEodParams,
    EodhdExchangesParams,
    EodhdSearchParams,
    eodhd_eod,
    eodhd_exchanges,
    eodhd_search,
)

_KEY = "live-looking-key-eodhd-xyz"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_env_vars_maps_api_key() -> None:
    assert CONNECTORS["eodhd_eod"].env_map == {"api_key": "EODHD_API_KEY"}


def test_connectors_count_matches_docstring() -> None:
    assert len(CONNECTORS) == 17


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


# ---------------------------------------------------------------------------
# eodhd_search (tool-tagged, carries error-mapping contract)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_eodhd_search_returns_rows() -> None:
    respx.get("https://eodhd.com/api/search/apple").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "Code": "AAPL",
                    "Exchange": "US",
                    "Name": "Apple Inc",
                    "Type": "Common Stock",
                    "Country": "USA",
                    "Currency": "USD",
                    "ISIN": "US0378331005",
                }
            ],
        )
    )

    bound = eodhd_search.bind(api_key=_KEY)
    result = await bound(EodhdSearchParams(query="apple"))

    assert result.provenance.source == "eodhd_search"
    df = result.data
    assert df.iloc[0]["Code"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_eodhd_search_maps_401_without_leaking_key() -> None:
    respx.get("https://eodhd.com/api/search/apple").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )

    bound = eodhd_search.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(EodhdSearchParams(query="apple"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_eodhd_search_maps_429_without_leaking_key() -> None:
    respx.get("https://eodhd.com/api/search/apple").mock(
        return_value=httpx.Response(429, headers={"Retry-After": "15"}, text="too many requests")
    )

    bound = eodhd_search.bind(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(EodhdSearchParams(query="apple"))
    assert _KEY not in str(exc_info.value)


# ---------------------------------------------------------------------------
# eodhd_eod
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_eodhd_eod_returns_ohlc_rows() -> None:
    respx.get("https://eodhd.com/api/eod/AAPL.US").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2026-04-18",
                    "open": 170.0,
                    "high": 172.0,
                    "low": 169.0,
                    "close": 171.5,
                    "adjusted_close": 171.5,
                    "volume": 45000000,
                }
            ],
        )
    )

    bound = eodhd_eod.bind(api_key=_KEY)
    result = await bound(EodhdEodParams(ticker="AAPL.US"))

    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["close"] == 171.5


# ---------------------------------------------------------------------------
# eodhd_exchanges (no path param, simple GET)
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_eodhd_exchanges_lists_exchanges() -> None:
    respx.get("https://eodhd.com/api/exchanges-list").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"Code": "US", "Name": "USA Exchanges", "Country": "USA", "Currency": "USD"},
                {"Code": "LSE", "Name": "London", "Country": "UK", "Currency": "GBP"},
            ],
        )
    )

    bound = eodhd_exchanges.bind(api_key=_KEY)
    result = await bound(EodhdExchangesParams())

    df = result.data
    assert set(df["Code"]) == {"US", "LSE"}


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_search_requires_query() -> None:
    with pytest.raises(ValueError):
        EodhdSearchParams()  # type: ignore[call-arg]
