"""Error-mapping contract for parsimony-eodhd."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_eodhd import EodhdSearchParams, eodhd_search


class TestEodhdSearchErrorMapping(ErrorMappingSuite):
    connector = eodhd_search
    params = EodhdSearchParams(query="apple")
    route_url = "https://eodhd.com/api/search/apple"
    provider = "eodhd"
