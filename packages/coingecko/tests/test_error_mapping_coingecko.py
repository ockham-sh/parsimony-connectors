"""Error-mapping contract for parsimony-coingecko."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_coingecko import coingecko_search


class TestCoinGeckoSearchErrorMapping(ErrorMappingSuite):
    connector = coingecko_search
    call_kwargs = {"query": "btc"}
    route_url = "https://api.coingecko.com/api/v3/search"
    provider = "coingecko"
