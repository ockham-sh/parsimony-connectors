"""Error-mapping contract for parsimony-fmp.

The canonical HTTP-status → ConnectorError table is asserted via the shared
``ErrorMappingSuite`` on ``fmp_search``. FMP overrides 403: invalid-key is
unambiguously 401, so 403 means a plan / legacy restriction → PaymentRequiredError
(not the canonical UnauthorizedError). That override — and the 402 mapping — are
covered in ``test_fmp_connectors.py``; here we pin the kernel-aligned statuses
(401, 402, 429, 5xx) plus the Retry-After + status_code contracts.
"""

from __future__ import annotations

from parsimony_test_support import ErrorMappingSuite

from parsimony_fmp import fmp_search


class TestFmpSearchErrorMapping(ErrorMappingSuite):
    connector = fmp_search
    call_kwargs = {"query": "apple"}
    route_url = "https://financialmodelingprep.com/stable/search-name"
    provider = "fmp"
