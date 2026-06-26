"""Error-mapping contract for parsimony-sec-edgar.

SEC EDGAR is keyless, so there is no API key to leak — but every request needs a
``User-Agent``, which the connector fast-fails on if absent. The autouse fixture
sets a throwaway UA so the suite exercises the HTTP-status → ConnectorError
mapping (which routes through the canonical ``fetch_json`` mappers) rather than
the UA fast-fail. ``env_key=None`` (public connector); ``submissions`` is the
representative GET verb.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from parsimony_test_support import ErrorMappingSuite

from parsimony_sec_edgar import sec_edgar_submissions


@pytest.fixture(autouse=True)
def _set_user_agent():
    with patch.dict(os.environ, {"SEC_EDGAR_USER_AGENT": "TestCo test@example.com"}):
        yield


class TestSecEdgarSubmissionsErrorMapping(ErrorMappingSuite):
    connector = sec_edgar_submissions
    call_kwargs = {"cik": "320193"}
    route_url = "https://data.sec.gov/submissions/CIK0000320193.json"
    method = "GET"
    env_key = None
    provider = "sec_edgar"
