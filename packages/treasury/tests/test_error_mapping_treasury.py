"""Error-mapping contract for parsimony-treasury."""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_treasury import TreasuryFetchParams, treasury_fetch


class TestTreasuryFetchErrorMapping(ErrorMappingSuite):
    connector = treasury_fetch
    params = TreasuryFetchParams(endpoint="v2/accounting/od/debt_to_penny")
    route_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
    env_key = None
