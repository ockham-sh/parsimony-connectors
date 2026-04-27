"""Live integration tests for parsimony-financial-reports.

The upstream ``financial_reports_generated_client`` SDK has a syntax
error in one of its generated files (``webhooks_management_api.py``)
that we don't exercise. The root ``pyproject.toml`` silences it for
mypy; we skip-guard it for pytest integration runs until the SDK is
patched upstream or the broken import is worked around in-package.
"""

from __future__ import annotations

import pytest
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_financial_reports import FrCompaniesSearchParams, fr_companies_search

pytestmark = pytest.mark.integration


def _sdk_importable() -> bool:
    try:
        import financial_reports_generated_client  # noqa: F401

        return True
    except Exception:
        return False


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _sdk_importable(),
    reason="financial_reports_generated_client SDK has an upstream SyntaxError; skipping until patched",
)
async def test_fr_companies_search_germany() -> None:
    creds = require_env("FINANCIAL_REPORTS_API_KEY")
    bound = fr_companies_search.bind(api_key=creds["FINANCIAL_REPORTS_API_KEY"])

    # Germany is large enough that the first page of search results is stable.
    result = await bound(FrCompaniesSearchParams(countries="DE", page_size=5))

    assert_provenance_shape(result)
    df = result.data
    assert not df.empty, "Financial Reports search DE returned empty DataFrame"
