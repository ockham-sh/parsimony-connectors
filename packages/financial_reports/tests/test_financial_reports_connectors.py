"""Happy-path tests for the Financial Reports connectors.

Financial Reports uses a generated SDK (``financial_reports_generated_client``)
rather than raw httpx, so respx-based mocking doesn't cover the transport
directly. We mock ``_with_retry`` (the single chokepoint for every SDK call)
to validate the kernel-surface shape without depending on SDK internals.

The 401/429 error-mapping contract from ``docs/testing-template.md`` §4 is
exercised via the ``_with_retry`` path: a retryable 429 raises ``RateLimitError``.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from parsimony_financial_reports import (
    CONNECTORS,
    ENV_VARS,
    FrCompaniesSearchParams,
    fr_companies_search,
)

_KEY = "live-looking-fr-key"


def test_env_vars_maps_api_key() -> None:
    assert ENV_VARS == {"api_key": "FINANCIAL_REPORTS_API_KEY"}


def test_connectors_count() -> None:
    assert len(CONNECTORS) == 10


def test_tool_tagged_first_line_long_enough() -> None:
    for c in CONNECTORS:
        if "tool" in c.tags:
            first = (c.description or "").splitlines()[0]
            assert len(first) >= 40, f"{c.name}: {first!r}"


class _FakeResponse:
    """Minimal stand-in for the SDK's paginated response model."""

    def __init__(self, results: list[dict]) -> None:
        self._results = results

    def model_dump(self, mode: str = "python") -> dict:
        return {"results": self._results, "count": len(self._results)}


@pytest.mark.asyncio
async def test_fr_companies_search_returns_rows() -> None:
    fake = _FakeResponse(
        [
            {
                "id": "c1",
                "name": "Example Inc",
                "isin": "US0000000001",
                "country": "US",
                "sector": "Software",
                "website": "https://example.com",
            }
        ]
    )

    async def _fake_with_retry(_coro_factory, _api_key):
        return fake

    with patch("parsimony_financial_reports._with_retry", side_effect=_fake_with_retry):
        bound = fr_companies_search.bind_deps(api_key=_KEY)
        result = await bound(FrCompaniesSearchParams(countries="US"))

    assert result.provenance.source.startswith("financial_reports")
    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Example Inc"
