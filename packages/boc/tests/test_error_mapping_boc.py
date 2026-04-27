"""Error-mapping contract for parsimony-boc (Bank of Canada)."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_boc import BocFetchParams, boc_fetch


class TestBocFetchErrorMapping(ErrorMappingSuite):
    connector = boc_fetch
    params = BocFetchParams(series_name="FXUSDCAD")
    route_url = "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json"
    env_key = None
