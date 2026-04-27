"""Error-mapping contract for parsimony-rba (Reserve Bank of Australia)."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_rba import RbaFetchParams, rba_fetch


class TestRbaFetchErrorMapping(ErrorMappingSuite):
    connector = rba_fetch
    params = RbaFetchParams(table_id="f1-data")
    # rba_fetch hits the table-index page first; error-mapping triggers there.
    route_url = "https://www.rba.gov.au/statistics/tables/"
    env_key = None
