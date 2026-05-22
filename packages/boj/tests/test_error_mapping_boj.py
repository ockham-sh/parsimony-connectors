"""Error-mapping contract for parsimony-boj (Bank of Japan)."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_boj import boj_fetch


class TestBojFetchErrorMapping(ErrorMappingSuite):
    connector = boj_fetch
    call_kwargs = {"db": "FM08", "code": "FXERD01"}
    route_url = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"
    env_key = None
