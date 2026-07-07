"""Error-mapping contract for parsimony-bdf.

``bdf_fetch`` routes HTTP errors through ``parsimony.transport.check_status``
(via ``fetch_json``); this pins the canonical status → ConnectorError mapping,
the Retry-After contract, and the no-leak defence. ``env_key`` defaults to
``api_key`` so the suite binds a canary key before driving the mocked statuses.
"""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_bdf.connectors.fetch import bdf_fetch

_OBSERVATIONS_URL = "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets/observations/exports/json"


class TestBdfFetchErrorMapping(ErrorMappingSuite):
    connector = bdf_fetch
    call_kwargs = {"key": "EXR.M.USD.EUR.SP00.E"}
    route_url = _OBSERVATIONS_URL
    provider = "bdf"
