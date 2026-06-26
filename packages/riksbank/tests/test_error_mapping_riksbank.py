"""Error-mapping contract for parsimony-riksbank."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_riksbank import riksbank_fetch


class TestRiksbankFetchErrorMapping(ErrorMappingSuite):
    connector = riksbank_fetch
    call_kwargs = {"series_id": "SEKEURPMI"}
    route_url = "https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI"
    provider = "riksbank"
