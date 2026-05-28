"""Happy-path tests for the simple FMP connectors.

FMP auth is ``?apikey=<key>`` via ``HttpClient(query_params=...)``; all
public-surface and docstring invariants live in ``test_public_surface.py``;
parametric error-mapping + no-leak invariants live in ``test_error_mapping.py``;
the screener's happy-path and feedback-loop behaviour lives in
``test_fmp_screener.py``.

This file covers the one simple-connector happy path that exercises
``fmp_fetch`` end-to-end (JSON → pandas DataFrame → OutputConfig → Result).
"""

from __future__ import annotations

import httpx
import pytest
import respx

from parsimony_fmp import (
    fmp_prices,
    fmp_search,
)

_KEY = "live-looking-fmp-key"


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

    bound = fmp_search.bind(api_key=_KEY)
    result = await bound(query="apple")

    assert result.provenance.source.startswith("fmp")
    assert result.data.iloc[0]["symbol"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_fmp_prices_intraday_preserves_time_component() -> None:
    """Regression: ``HISTORICAL_PRICES_OUTPUT.date`` must be ``datetime``,
    not ``date``. ``date`` runs ``dt.normalize()`` which would zero out
    the time component on intraday rows (1min/5min/.../4hour)."""
    respx.get("https://financialmodelingprep.com/stable/historical-chart/1min").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "date": "2026-05-08 14:30:00",
                    "open": 200.10,
                    "high": 200.50,
                    "low": 200.05,
                    "close": 200.40,
                    "volume": 12345,
                },
                {
                    "date": "2026-05-08 14:29:00",
                    "open": 199.95,
                    "high": 200.20,
                    "low": 199.90,
                    "close": 200.10,
                    "volume": 9876,
                },
            ],
        )
    )

    bound = fmp_prices.bind(api_key=_KEY)
    result = await bound(symbol="AAPL", frequency="1min")

    times = result.data["date"].dt.time.astype(str).tolist()
    assert "00:00:00" not in times, f"intraday timestamps were normalized to midnight: {times}"
    assert times[0] == "14:30:00"
    assert times[1] == "14:29:00"
