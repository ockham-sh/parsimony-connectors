"""Error-mapping contract for parsimony-eia."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_eia import EiaFetchParams, eia_fetch


class TestEiaFetchErrorMapping(ErrorMappingSuite):
    connector = eia_fetch
    params = EiaFetchParams(route="petroleum/pri/spt")
    route_url = "https://api.eia.gov/v2/petroleum/pri/spt/data"
    provider = "eia"
