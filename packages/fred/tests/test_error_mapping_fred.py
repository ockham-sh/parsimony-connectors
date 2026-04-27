"""Error-mapping contract for parsimony-fred.

FRED connectors route HTTP errors through ``parsimony.transport.map_http_error``;
this file pins the canonical mapping via :class:`ErrorMappingSuite`.
"""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_fred import FredFetchParams, FredSearchParams, fred_fetch, fred_search


class TestFredSearchErrorMapping(ErrorMappingSuite):
    connector = fred_search
    params = FredSearchParams(search_text="unemployment")
    route_url = "https://api.stlouisfed.org/fred/series/search"
    provider = "fred"


class TestFredFetchErrorMapping(ErrorMappingSuite):
    connector = fred_fetch
    params = FredFetchParams(series_id="UNRATE")
    route_url = "https://api.stlouisfed.org/fred/series/observations"
    provider = "fred"
