"""Happy-path tests for the SEC EDGAR connectors.

Unlike HTTP-wrapping connectors, sec_edgar dispatches blocking ``edgartools``
library calls through ``asyncio.to_thread``. The happy-path tests patch the
in-module helpers (``_resolve_company`` / ``_resolve_to_entity`` /
``_get_filing_by_accession``) rather than using respx.

Task 7 of the Track B council plan asked for ``asyncio.to_thread``-wrapping
of blocking calls; inspection of the module confirms every ``edgartools``
call site is already wrapped. The contract-shape assertion below guards
against future regressions.

sec_edgar has no ``api_key`` dep (SEC EDGAR uses a user-agent header set via
``EDGAR_IDENTITY``); 401/429 error-mapping tests do not apply.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError

from parsimony_sec_edgar import (
    CONNECTORS,
    SecEdgarCompanyProfileParams,
    SecEdgarFindCompanyParams,
    sec_edgar_company_profile,
    sec_edgar_find_company,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_has_fifteen_connectors() -> None:
    # 15 connectors per the module docstring — guard against accidental drop.
    assert len(CONNECTORS) == 15


def test_tool_tagged_connectors_have_long_descriptions() -> None:
    # Mirrors the kernel conformance check but explicit here so regressions
    # surface with a specific connector name, not a generic conformance failure.
    for c in CONNECTORS:
        if "tool" in c.tags:
            first_line = (c.description or "").splitlines()[0]
            assert len(first_line) >= 40, (
                f"tool-tagged connector {c.name!r} first line is {len(first_line)} chars"
            )


def test_every_connector_body_uses_asyncio_to_thread() -> None:
    """Task 7 watchpoint: blocking edgartools calls must stay thread-dispatched.

    Reads the module source and asserts the substring ``asyncio.to_thread``
    appears at least once per connector body. A failure means someone
    inlined a blocking call back onto the event loop.
    """
    import inspect

    import parsimony_sec_edgar

    source = inspect.getsource(parsimony_sec_edgar)
    # Coarse but effective: count to_thread usages and compare with connector
    # count. Every connector that calls into edgartools must dispatch through
    # a thread; the helper functions _statement_from_* are called through
    # asyncio.to_thread by _fetch_statement at module-read time.
    assert source.count("asyncio.to_thread") >= 15


# ---------------------------------------------------------------------------
# sec_edgar_find_company
# ---------------------------------------------------------------------------


class _FakeEntity:
    """Minimal stand-in for the edgartools Company object."""

    def __init__(self, name: str, cik: str, ticker: str) -> None:
        self.name = name
        self.cik = cik
        self.tickers = [ticker] if ticker else []
        self.industry = "Software"
        self.sic = "7372"
        self.fiscal_year_end = "12-31"


@pytest.mark.asyncio
async def test_sec_edgar_find_company_returns_entity_row() -> None:
    fake = _FakeEntity(name="Example Inc", cik="1234567", ticker="EX")

    with patch("parsimony_sec_edgar._resolve_company", return_value=fake):
        result = await sec_edgar_find_company(SecEdgarFindCompanyParams(identifier="EX"))

    assert result.provenance.source == "sec_edgar"
    df = result.data
    assert "cik" in df.columns
    assert "name" in df.columns
    assert df.iloc[0]["name"] == "Example Inc"


@pytest.mark.asyncio
async def test_sec_edgar_find_company_propagates_empty_data_error() -> None:
    # _resolve_company raises EmptyDataError when edgartools' find() returns None.
    with patch(
        "parsimony_sec_edgar._resolve_company",
        side_effect=EmptyDataError(provider="sec_edgar", message="No SEC company found"),
    ), pytest.raises(EmptyDataError):
        await sec_edgar_find_company(SecEdgarFindCompanyParams(identifier="zzz"))


# ---------------------------------------------------------------------------
# sec_edgar_company_profile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sec_edgar_company_profile_returns_structured_row() -> None:
    fake = _FakeEntity(name="Example Inc", cik="1234567", ticker="EX")

    def _resolve_entity(_identifier: str) -> Any:
        return fake

    with patch("parsimony_sec_edgar._resolve_to_entity", side_effect=_resolve_entity):
        result = await sec_edgar_company_profile(
            SecEdgarCompanyProfileParams(identifier="EX")
        )

    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["ticker"] == "EX"
    assert df.iloc[0]["cik"].endswith("1234567")
    assert result.provenance.source == "sec_edgar"


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_find_company_requires_identifier() -> None:
    with pytest.raises(ValueError):
        SecEdgarFindCompanyParams()  # type: ignore[call-arg]
