"""Error-mapping contract for parsimony-eia.

``eia_fetch`` routes HTTP errors through a per-package chokepoint (``eia_get``):
the canonical statuses (401/402/429/5xx) go through ``check_status`` and pin
the standard ConnectorError table + the no-leak defence; the non-canonical
**400** carries EIA's useful JSON error body, so it maps to a message-preserving
``InvalidParameterError`` (asserted separately below). ``env_key`` defaults to
``api_key`` so the suite binds a canary key before driving the mocked statuses.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import InvalidParameterError
from parsimony_test_support import CANARY_KEY, ErrorMappingSuite, assert_no_secret_leak

from parsimony_eia.connectors.fetch import eia_fetch

_ROUTE_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data"


class TestEiaFetchErrorMapping(ErrorMappingSuite):
    connector = eia_fetch
    call_kwargs = {"route": "petroleum/pri/spt"}
    route_url = _ROUTE_URL
    provider = "eia"


@respx.mock
def test_eia_400_maps_to_invalid_parameter_preserving_message() -> None:
    """The non-canonical 400 → InvalidParameterError, keeping EIA's body text and
    never leaking the key."""
    respx.get(_ROUTE_URL).mock(
        return_value=httpx.Response(
            400, json={"error": "Invalid frequency 'millenially' provided.", "code": 400}
        )
    )
    with pytest.raises(InvalidParameterError) as exc_info:
        eia_fetch.bind(api_key=CANARY_KEY)(route="petroleum/pri/spt", frequency="millenially")
    assert "Invalid frequency" in str(exc_info.value)
    assert_no_secret_leak(exc_info.value, secret=CANARY_KEY)
