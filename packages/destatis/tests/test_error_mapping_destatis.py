"""Error-mapping contract for parsimony-destatis."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_destatis.connectors.fetch import destatis_fetch

_DATA_URL = "https://genesis.destatis.de/genesis/api/rest/tables/61111-0001/data"


class TestDestatisFetchErrorMapping(ErrorMappingSuite):
    connector = destatis_fetch
    call_kwargs = {"name": "61111-0001"}
    route_url = _DATA_URL
    env_key = None
    provider = "destatis"
