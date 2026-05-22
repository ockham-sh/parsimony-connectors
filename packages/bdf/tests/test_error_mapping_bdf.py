"""Error-mapping contract for parsimony-bdf."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_bdf import bdf_fetch

_OBSERVATIONS_URL = (
    "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets"
    "/observations/exports/json"
)


class TestBdfFetchErrorMapping(ErrorMappingSuite):
    connector = bdf_fetch
    call_kwargs = {"key": "EXR.M.USD.EUR.SP00.E"}
    route_url = _OBSERVATIONS_URL
    provider = "bdf"
