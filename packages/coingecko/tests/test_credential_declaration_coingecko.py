"""Credential-declaration contract for parsimony-coingecko.

One :class:`CredentialDeclarationSuite` subclass per HTTP-calling verb. Each
proves the connector's ``requires=("COINGECKO_API_KEY",)`` declaration matches
runtime: the bare call fast-fails naming that env var without touching the
network, and an env-supplied key reaches the outgoing request (CoinGecko carries
it in the ``x-cg-demo-api-key`` header).
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_coingecko import (
    coingecko_coin_detail,
    coingecko_market_chart,
    coingecko_market_chart_range,
    coingecko_markets,
    coingecko_ohlc,
    coingecko_price,
    coingecko_search,
    coingecko_token_price_onchain,
    coingecko_top_gainers_losers,
    coingecko_trending,
    enumerate_coingecko,
)

_BASE = "https://api.coingecko.com/api/v3"


class TestCoinGeckoSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_search
    call_kwargs = {"query": "btc"}
    route_url = f"{_BASE}/search"


class TestCoinGeckoTrendingCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_trending
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/search/trending"


class TestCoinGeckoTopGainersLosersCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_top_gainers_losers
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/coins/top_gainers_losers"


class TestCoinGeckoPriceCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_price
    call_kwargs = {"ids": "bitcoin"}
    route_url = f"{_BASE}/simple/price"


class TestCoinGeckoMarketsCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_markets
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/coins/markets"


class TestCoinGeckoCoinDetailCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_coin_detail
    call_kwargs = {"coin_id": "bitcoin"}
    route_url = f"{_BASE}/coins/bitcoin"


class TestCoinGeckoMarketChartCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_market_chart
    call_kwargs = {"coin_id": "bitcoin", "days": "1"}
    route_url = f"{_BASE}/coins/bitcoin/market_chart"


class TestCoinGeckoMarketChartRangeCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_market_chart_range
    call_kwargs = {"coin_id": "bitcoin", "from_date": "2024-01-01", "to_date": "2024-01-02"}
    route_url = f"{_BASE}/coins/bitcoin/market_chart/range"


class TestCoinGeckoOhlcCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_ohlc
    call_kwargs = {"coin_id": "bitcoin"}
    route_url = f"{_BASE}/coins/bitcoin/ohlc"


class TestCoinGeckoTokenPriceOnchainCredentialDeclaration(CredentialDeclarationSuite):
    connector = coingecko_token_price_onchain
    call_kwargs = {"network": "eth", "contract_addresses": "0xabc"}
    route_url = f"{_BASE}/onchain/simple/networks/eth/token_price/0xabc"


class TestCoinGeckoEnumerateCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_coingecko
    call_kwargs: dict[str, object] = {}
    route_url = f"{_BASE}/coins/list"
