"""Error-mapping contract for parsimony-fred.

FRED connectors map HTTP statuses through ``parsimony.transport.check_status``;
this file pins the canonical mapping via :class:`ErrorMappingSuite`.
"""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_fred import fred_fetch, fred_search


class TestFredSearchErrorMapping(ErrorMappingSuite):
    connector = fred_search
    call_kwargs = {"search_text": "unemployment"}
    route_url = "https://api.stlouisfed.org/fred/series/search"
    provider = "fred"


class TestFredFetchErrorMapping(ErrorMappingSuite):
    connector = fred_fetch
    call_kwargs = {"series_id": "UNRATE"}
    route_url = "https://api.stlouisfed.org/fred/series/observations"
    provider = "fred"
