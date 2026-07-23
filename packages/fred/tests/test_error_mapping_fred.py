"""Error-mapping contract for parsimony-fred.

FRED connectors map HTTP statuses through ``parsimony.transport.check_status``;
this file pins the canonical mapping via :class:`ErrorMappingSuite`.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import InvalidParameterError
from parsimony_test_support import CANARY_KEY, ErrorMappingSuite

from parsimony_fred import fred_fetch, fred_search


class TestFredSearchErrorMapping(ErrorMappingSuite):
    connector = fred_search
    call_kwargs = {"query": "unemployment"}
    route_url = "https://api.stlouisfed.org/fred/series/search"
    provider = "fred"


class TestFredFetchErrorMapping(ErrorMappingSuite):
    connector = fred_fetch
    call_kwargs = {"series_id": "UNRATE"}
    route_url = "https://api.stlouisfed.org/fred/series/observations"
    provider = "fred"


@respx.mock
def test_fred_fetch_actionable_400_preserves_message() -> None:
    # FRED answers a bad series_id/parameter with a 400 naming the problem under
    # ``error_message``; the connector must surface that, not an opaque 400.
    respx.route(method="GET", url="https://api.stlouisfed.org/fred/series/observations").mock(
        return_value=httpx.Response(
            400,
            json={"error_code": 400, "error_message": "Bad Request. Invalid value for variable series_id."},
        )
    )
    with pytest.raises(InvalidParameterError) as exc:
        fred_fetch.bind(api_key=CANARY_KEY)(series_id="THIS_IS_NOT_A_SERIES")
    assert "Invalid value for variable series_id" in str(exc.value)
    assert exc.value.provider == "fred"
    assert CANARY_KEY not in str(exc.value)
