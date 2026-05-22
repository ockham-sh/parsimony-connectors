"""Happy-path tests for the Alpha Vantage connectors.

Alpha Vantage exposes 29 connectors through a single ``/query`` endpoint
differentiated by a ``function`` query param. The shared ``_av_fetch`` helper
owns the error-mapping contract — 401/403 → UnauthorizedError, 429 →
RateLimitError, 'Note'/'Information' in body → RateLimitError/
PaymentRequiredError. We test the error paths through one connector
(``alpha_vantage_search``) rather than 29 times.

Following ``docs/testing-template.md`` §4: api_key value must not leak into
raised exceptions.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import RateLimitError, UnauthorizedError

from parsimony_alpha_vantage import (
    CONNECTORS,
    alpha_vantage_daily,
    alpha_vantage_fx_rate,
    alpha_vantage_search,
    alpha_vantage_technical,
)

_KEY = "live-looking-av-key-xyz"

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_count() -> None:
    # 28 connectors + 1 enumerator per the module docstring.
    assert len(CONNECTORS) == 29


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


# ---------------------------------------------------------------------------
# alpha_vantage_search — carries the HTTP-error-mapping contract
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_search_returns_best_matches() -> None:
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(
            200,
            json={
                "bestMatches": [
                    {
                        "1. symbol": "AAPL",
                        "2. name": "Apple Inc",
                        "3. type": "Equity",
                        "4. region": "United States",
                        "5. marketOpen": "09:30",
                        "6. marketClose": "16:00",
                        "7. timezone": "UTC-04",
                        "8. currency": "USD",
                        "9. matchScore": "1.0000",
                    }
                ]
            },
        )
    )

    bound = alpha_vantage_search.bind(api_key=_KEY)
    result = await bound(keywords="apple")

    # Alpha Vantage uses per-endpoint provenance sources; confirm the prefix.
    assert result.provenance.source.startswith("alpha_vantage")
    assert result.data.iloc[0]["symbol"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_search_maps_401_without_leaking_key() -> None:
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )

    bound = alpha_vantage_search.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(keywords="x")
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_search_maps_429_without_leaking_key() -> None:
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(429, text="too many requests")
    )

    bound = alpha_vantage_search.bind(api_key=_KEY)
    with pytest.raises(RateLimitError) as exc_info:
        await bound(keywords="x")
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_search_maps_in_body_rate_limit_note() -> None:
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(
            200,
            json={"Note": "Thank you for using Alpha Vantage! Our standard API rate limit..."},
        )
    )

    bound = alpha_vantage_search.bind(api_key=_KEY)
    with pytest.raises(RateLimitError):
        await bound(keywords="x")


# ---------------------------------------------------------------------------
# alpha_vantage_daily
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_daily_returns_ohlcv_rows() -> None:
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(
            200,
            json={
                "Meta Data": {"2. Symbol": "AAPL"},
                "Time Series (Daily)": {
                    "2026-04-18": {
                        "1. open": "170.00",
                        "2. high": "172.00",
                        "3. low": "169.00",
                        "4. close": "171.50",
                        "5. volume": "45000000",
                    },
                },
            },
        )
    )

    bound = alpha_vantage_daily.bind(api_key=_KEY)
    result = await bound(symbol="AAPL")

    df = result.data
    assert len(df) >= 1
    assert "close" in df.columns


# ---------------------------------------------------------------------------
# alpha_vantage_fx_rate
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fx_rate_returns_single_row() -> None:
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(
            200,
            json={
                "Realtime Currency Exchange Rate": {
                    "1. From_Currency Code": "USD",
                    "2. From_Currency Name": "United States Dollar",
                    "3. To_Currency Code": "EUR",
                    "4. To_Currency Name": "Euro",
                    "5. Exchange Rate": "0.9234",
                    "6. Last Refreshed": "2026-04-18 14:00:00",
                    "7. Time Zone": "UTC",
                    "8. Bid Price": "0.9233",
                    "9. Ask Price": "0.9235",
                }
            },
        )
    )

    bound = alpha_vantage_fx_rate.bind(api_key=_KEY)
    result = await bound(from_currency="USD", to_currency="EUR")

    df = result.data
    assert len(df) == 1


@respx.mock
@pytest.mark.asyncio
async def test_technical_intraday_preserves_time_component() -> None:
    """Regression: ``TECHNICAL_OUTPUT.date`` must be ``datetime``, not
    ``date``. ``date`` runs ``dt.normalize()`` which would zero out the
    time component when the technical endpoint is called with intraday
    intervals (1min/5min/15min/30min/60min)."""
    respx.get("https://www.alphavantage.co/query").mock(
        return_value=httpx.Response(
            200,
            json={
                "Meta Data": {"1: Symbol": "AAPL", "4: Interval": "1min"},
                "Technical Analysis: SMA": {
                    "2026-05-08 14:30:00": {"SMA": "200.3400"},
                    "2026-05-08 14:29:00": {"SMA": "200.2100"},
                },
            },
        )
    )

    bound = alpha_vantage_technical.bind(api_key=_KEY)
    result = await bound(
        symbol="AAPL",
        function="SMA",
        interval="1min",
        time_period=20,
        series_type="close",
    )

    times = result.data["date"].dt.time.astype(str).tolist()
    assert "00:00:00" not in times, f"intraday timestamps were normalized to midnight: {times}"
    assert set(times) == {"14:30:00", "14:29:00"}
