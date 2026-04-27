"""Error-mapping contract for parsimony-tiingo."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_tiingo import TiingoSearchParams, tiingo_search


class TestTiingoSearchErrorMapping(ErrorMappingSuite):
    connector = tiingo_search
    params = TiingoSearchParams(query="apple")
    route_url = "https://api.tiingo.com/tiingo/utilities/search"
    provider = "tiingo"
