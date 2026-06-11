"""Live integration tests for parsimony-sec-edgar.

SEC EDGAR is keyless but its fair-access policy *requires* a ``User-Agent``
header identifying the requester (name + email). The connector reads it from
the mandatory ``SEC_EDGAR_USER_AGENT`` env var. These tests
``require_env("SEC_EDGAR_USER_AGENT")`` and therefore **skip cleanly** when the
var is unset — a clean skip is the expected outcome in environments without a
configured identity, not a failure.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/sec_edgar -m integration
"""

from __future__ import annotations

import pytest
from parsimony.errors import EmptyDataError
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_sec_edgar import (
    sec_edgar_company_facts,
    sec_edgar_fetch_filing,
    sec_edgar_find_company,
    sec_edgar_submissions,
)

pytestmark = pytest.mark.integration

# Apple is the canonical fixture — CIK 0000320193, ticker AAPL.
_APPLE_CIK = "320193"
_APPLE_CIK_PADDED = "0000320193"


def test_find_company_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_find_company(identifier="AAPL")

    assert_provenance_shape(result, expected_source="sec_edgar_find_company", required_param_keys=["identifier"])
    df = result.data
    assert not df.empty, "sec_edgar_find_company('AAPL') returned empty DataFrame"
    assert list(df.columns) == ["cik", "title", "ticker"]
    # Real content: AAPL must resolve to Apple's CIK and a non-empty title.
    aapl = df[df["ticker"] == "AAPL"]
    assert not aapl.empty, "AAPL ticker not present in matches"
    assert aapl.iloc[0]["cik"] == _APPLE_CIK_PADDED, "AAPL did not resolve to CIK 0000320193"
    assert "APPLE" in aapl.iloc[0]["title"].upper(), "title is not Apple's company name"


def test_submissions_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_submissions(cik=_APPLE_CIK, limit=10)

    assert_provenance_shape(result, expected_source="sec_edgar_submissions", required_param_keys=["cik"])
    df = result.data
    assert not df.empty, "Apple submissions returned empty DataFrame"
    assert list(df.columns) == ["accessionNumber", "filingDate", "form", "primaryDocument"]
    assert len(df) <= 10, "limit not respected"
    # Real content: accession numbers and form types must be populated.
    assert df["accessionNumber"].astype(str).str.len().gt(0).all(), "blank accession number"
    assert df["form"].astype(str).str.len().gt(0).any(), "no real form type"
    assert df["filingDate"].astype(str).str.match(r"\d{4}-\d{2}-\d{2}").any(), "no real filing date"


def test_company_facts_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_company_facts(cik=_APPLE_CIK)

    assert_provenance_shape(result, expected_source="sec_edgar_company_facts", required_param_keys=["cik"])
    facts = result.data
    assert isinstance(facts, dict), f"expected a facts dict, got {type(facts)!r}"
    assert facts.get("cik") == 320193, f"unexpected cik in facts: {facts.get('cik')!r}"
    assert "APPLE" in str(facts.get("entityName", "")).upper(), "entityName is not Apple"
    # Real content: us-gaap taxonomy with at least one reported concept.
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    assert us_gaap, "no us-gaap facts returned"
    assert "Assets" in us_gaap or len(us_gaap) > 10, "us-gaap facts look unpopulated"


def test_fetch_filing_apple_latest() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    # Resolve a real, recent accession from the live submissions feed rather
    # than hardcoding one (filings roll forward over time).
    subs = sec_edgar_submissions(cik=_APPLE_CIK, limit=20)
    accession = str(subs.data.iloc[0]["accessionNumber"])

    result = sec_edgar_fetch_filing(cik=_APPLE_CIK, accession_number=accession)

    assert_provenance_shape(
        result, expected_source="sec_edgar_fetch_filing", required_param_keys=["cik", "accession_number"]
    )
    data = result.data
    assert isinstance(data, dict), f"expected a filing dict, got {type(data)!r}"
    assert data["cik"] == _APPLE_CIK_PADDED
    assert data["document"], "no document name resolved"
    # Real content: the document body must be a non-trivial text/HTML blob.
    assert len(data["content"]) > 100, f"filing content too short ({len(data['content'])} chars)"


def test_find_company_no_match_raises_empty() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    # A ticker that does not exist exercises the live EmptyDataError path.
    with pytest.raises(EmptyDataError):
        sec_edgar_find_company(identifier="ZZZZZ_NO_SUCH_TICKER_XYZ")
