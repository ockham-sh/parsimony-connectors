"""Happy-path tests for SEC EDGAR HTTP connectors."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError, UnauthorizedError

from parsimony_sec_edgar import CONNECTORS, sec_edgar_find_company, sec_edgar_submissions


def test_connectors_collection_has_four_connectors() -> None:
    assert len(CONNECTORS) == 4


@pytest.mark.asyncio
async def test_sec_edgar_find_company_returns_match() -> None:
    tickers = [{"cik_str": 789, "ticker": "EX", "title": "Example Inc"}]

    with (
        patch.dict("os.environ", {"SEC_EDGAR_USER_AGENT": "TestCo test@example.com"}),
        patch("parsimony_sec_edgar._load_company_tickers", new=AsyncMock(return_value=tickers)),
    ):
        result = await sec_edgar_find_company(identifier="EX")

    assert result.provenance.source == "sec_edgar_find_company"
    assert result.data.iloc[0]["ticker"] == "EX"


@pytest.mark.asyncio
async def test_sec_edgar_find_company_empty() -> None:
    with (
        patch.dict("os.environ", {"SEC_EDGAR_USER_AGENT": "TestCo test@example.com"}),
        patch("parsimony_sec_edgar._load_company_tickers", new=AsyncMock(return_value=[])),
        pytest.raises(EmptyDataError),
    ):
        await sec_edgar_find_company(identifier="missing")


@pytest.mark.asyncio
async def test_sec_edgar_submissions_returns_rows() -> None:
    payload = {
        "filings": {
            "recent": {
                "accessionNumber": ["0001"],
                "filingDate": ["2024-01-01"],
                "form": ["10-K"],
                "primaryDocument": ["doc.htm"],
            }
        }
    }

    with (
        patch.dict("os.environ", {"SEC_EDGAR_USER_AGENT": "TestCo test@example.com"}),
        patch("parsimony_sec_edgar._get_json", new=AsyncMock(return_value=payload)),
    ):
        result = await sec_edgar_submissions(cik="789")

    assert isinstance(result.data, pd.DataFrame)
    assert result.data.iloc[0]["form"] == "10-K"


def test_missing_user_agent_raises() -> None:
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError):
        from parsimony_sec_edgar import _user_agent

        _user_agent()
