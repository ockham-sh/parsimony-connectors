"""Error-mapping contract for parsimony-alpha-vantage."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_alpha_vantage import AlphaVantageSearchParams, alpha_vantage_search


class TestAlphaVantageSearchErrorMapping(ErrorMappingSuite):
    connector = alpha_vantage_search
    params = AlphaVantageSearchParams(keywords="apple")
    route_url = "https://www.alphavantage.co/query"
    provider = "alpha_vantage"
