"""Error-mapping contract for parsimony-snb (Swiss National Bank)."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_snb import SnbFetchParams, snb_fetch


class TestSnbFetchErrorMapping(ErrorMappingSuite):
    connector = snb_fetch
    params = SnbFetchParams(cube_id="rendoblim")
    route_url = "https://data.snb.ch/api/cube/rendoblim/data/csv/en"
    env_key = None
