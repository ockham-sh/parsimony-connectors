"""Error-mapping contract for parsimony-bde."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_bde import bde_fetch


class TestBdeFetchErrorMapping(ErrorMappingSuite):
    connector = bde_fetch
    call_kwargs = {"key": "D_1NBAF472"}
    route_url = "https://app.bde.es/bierest/resources/srdatosapp/listaSeries"
    env_key = None
