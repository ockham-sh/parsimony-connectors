"""Error-mapping contract for parsimony-bls.

``bls_fetch`` POSTs JSON to the API. The ``api_key`` dep is optional on BLS's
side but the connector still declares it as a secret — we bind a sentinel and
assert it never leaks while HTTP errors map to the typed-error taxonomy.
"""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_bls.connectors.fetch import bls_fetch


class TestBlsFetchErrorMapping(ErrorMappingSuite):
    connector = bls_fetch
    call_kwargs = {"series_id": "LNS14000000", "start_year": "2026", "end_year": "2026"}
    route_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    method = "POST"
    provider = "bls"
