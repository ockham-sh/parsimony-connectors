"""Error-mapping contract for parsimony-treasury."""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import InvalidParameterError
from parsimony_test_support import ErrorMappingSuite

from parsimony_treasury import treasury_fetch


class TestTreasuryFetchErrorMapping(ErrorMappingSuite):
    connector = treasury_fetch
    call_kwargs = {"endpoint": "v2/accounting/od/debt_to_penny"}
    route_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny"
    env_key = None


@respx.mock
def test_treasury_fetch_actionable_400_preserves_message() -> None:
    # Fiscal Data answers a bad field/filter with a 400 whose ``message`` names
    # the offending parameter; the connector must surface that, not an opaque 400.
    endpoint = "v2/accounting/od/avg_interest_rates"
    respx.route(
        method="GET",
        url=f"https://api.fiscaldata.treasury.gov/services/api/fiscal_service/{endpoint}",
    ).mock(
        return_value=httpx.Response(
            400,
            json={
                "error": "Invalid Query Param",
                "message": "Invalid query parameter: Field 'NOPE' does not exist.",
            },
        )
    )
    with pytest.raises(InvalidParameterError) as exc:
        treasury_fetch(endpoint=endpoint, filter="NOPE:eq:1")
    assert "Field 'NOPE' does not exist." in str(exc.value)
    assert exc.value.provider == "treasury"
