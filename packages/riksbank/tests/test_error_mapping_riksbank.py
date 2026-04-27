"""Error-mapping contract for parsimony-riksbank."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_riksbank import RiksbankFetchParams, riksbank_fetch


class TestRiksbankFetchErrorMapping(ErrorMappingSuite):
    connector = riksbank_fetch
    params = RiksbankFetchParams(series_id="SEKEURPMI")
    route_url = "https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI"
    provider = "riksbank"
