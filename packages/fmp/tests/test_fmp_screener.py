"""Happy-path and error-mapping tests for ``fmp_screener``.

The screener fans out to three FMP endpoints and joins the results. These
tests cover the shape of the join, the 401 no-leak invariant, the 402
mapping to ``PaymentRequiredError``, and the "skip enrichment when fields
are screener-native only" short-circuit.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import PaymentRequiredError, UnauthorizedError

from parsimony_fmp import FmpScreenerParams, fmp_screener

_KEY = "live-looking-fmp-screener-key"


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

    bound = fmp_screener.bind(api_key=_KEY)
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

    bound = fmp_screener.bind(api_key=_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        await bound(FmpScreenerParams(sector="Technology"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_fmp_screener_maps_402_to_payment_required() -> None:
    """HTTP 402 from the screener endpoint must raise ``PaymentRequiredError``."""
    respx.get("https://financialmodelingprep.com/stable/company-screener").mock(
        return_value=httpx.Response(402, text="plan upgrade required")
    )

    bound = fmp_screener.bind(api_key=_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        await bound(FmpScreenerParams(sector="Technology"))
    assert _KEY not in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_fmp_screener_skips_enrichment_when_fields_are_native_only() -> None:
    """Zero enrichment calls when ``fields`` references only screener-native columns.

    If a caller passes ``fields=["symbol", "companyName", "marketCap"]`` the
    screener must hit ``company-screener`` exactly once and skip both
    ``key-metrics-ttm`` and ``ratios-ttm`` entirely.
    """
    screener_route = respx.get("https://financialmodelingprep.com/stable/company-screener").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"symbol": "AAPL", "companyName": "Apple Inc", "marketCap": 2_800_000_000_000},
                {"symbol": "MSFT", "companyName": "Microsoft Corp", "marketCap": 2_700_000_000_000},
            ],
        )
    )
    metrics_route = respx.get("https://financialmodelingprep.com/stable/key-metrics-ttm")
    ratios_route = respx.get("https://financialmodelingprep.com/stable/ratios-ttm")

    bound = fmp_screener.bind(api_key=_KEY)
    result = await bound(
        FmpScreenerParams(
            sector="Technology",
            fields=["symbol", "companyName", "marketCap"],
        )
    )

    assert screener_route.call_count == 1
    assert metrics_route.call_count == 0, "key-metrics-ttm must be skipped when fields are native-only"
    assert ratios_route.call_count == 0, "ratios-ttm must be skipped when fields are native-only"
    assert len(result.data) == 2
