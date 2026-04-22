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

from parsimony_fmp import FmpSearchParams, fmp_search

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
    result = await bound(FmpSearchParams(query="apple"))

    assert result.provenance.source.startswith("fmp")
    assert result.data.iloc[0]["symbol"] == "AAPL"
