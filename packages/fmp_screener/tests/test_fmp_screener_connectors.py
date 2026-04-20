"""Happy-path tests for the FMP Screener connector.

Single connector (``fmp_screener``) that fans out to three FMP endpoints
(company-screener, key-metrics-ttm, financial-ratios-ttm) and joins results.
Test a minimal happy path + api_key non-leakage on 401.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import UnauthorizedError

from parsimony_fmp_screener import CONNECTORS, FmpScreenerParams, fmp_screener

_KEY = "live-looking-fmp-screener-key"


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"fmp_screener"}


def test_connector_is_tool_tagged() -> None:
    assert "tool" in fmp_screener.tags


@respx.mock
@pytest.mark.asyncio
async def test_fmp_screener_joins_screener_and_enrichment() -> None:
    respx.get("https://financialmodelingprep.com/stable/company-screener").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "symbol": "AAPL",
                    "companyName": "Apple Inc",
                    "marketCap": 2800000000000,
                    "price": 171.5,
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "exchange": "NASDAQ",
                    "country": "US",
                    "isActivelyTrading": True,
                }
            ],
        )
    )
    respx.get("https://financialmodelingprep.com/stable/key-metrics-ttm").mock(
        return_value=httpx.Response(200, json=[{"symbol": "AAPL", "peRatioTTM": 28.5}])
    )
    respx.get("https://financialmodelingprep.com/stable/ratios-ttm").mock(
        return_value=httpx.Response(200, json=[{"symbol": "AAPL", "grossProfitMarginTTM": 0.45}])
    )

    bound = fmp_screener.bind_deps(api_key=_KEY)
    result = await bound(FmpScreenerParams(sector="Technology"))

    assert result.provenance.source.startswith("fmp")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "AAPL"


@respx.mock
@pytest.mark.asyncio
async def test_fmp_screener_maps_401_without_leaking_key() -> None:
    respx.get("https://financialmodelingprep.com/stable/company-screener").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )

    bound = fmp_screener.bind_deps(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(FmpScreenerParams(sector="Technology"))
    assert _KEY not in str(exc_info.value)
