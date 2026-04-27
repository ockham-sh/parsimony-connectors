"""Error-mapping contract for parsimony-bls.

BLS uses POST with JSON body. The ``api_key`` dep is optional on BLS's
side but required by the connector contract — we bind a sentinel and
assert it never leaks.
"""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_bls import BlsFetchParams, bls_fetch


class TestBlsFetchErrorMapping(ErrorMappingSuite):
    connector = bls_fetch
    params = BlsFetchParams(series_id="LNS14000000", start_year="2025", end_year="2026")
    route_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    method = "POST"
    provider = "bls"
