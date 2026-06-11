"""Offline tests for SEC EDGAR connectors.

These exercise every verb's happy path, the empty/parse guards, the inline
parameter validation, and the missing-User-Agent fast-fail — all without
touching the network (HTTP is mocked). Live behaviour is covered separately
in ``test_integration_sec_edgar.py`` (skips when SEC_EDGAR_USER_AGENT is
unset).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pandas as pd
import pytest
import respx
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    UnauthorizedError,
)

from parsimony_sec_edgar import (
    CONNECTORS,
    sec_edgar_company_facts,
    sec_edgar_fetch_filing,
    sec_edgar_find_company,
    sec_edgar_submissions,
)

_UA = {"SEC_EDGAR_USER_AGENT": "TestCo test@example.com"}


# ---------------------------------------------------------------------------
# Collection / shape
# ---------------------------------------------------------------------------


def test_connectors_collection_has_four_connectors() -> None:
    assert len(CONNECTORS) == 4
    assert set(CONNECTORS.names()) == {
        "sec_edgar_find_company",
        "sec_edgar_submissions",
        "sec_edgar_company_facts",
        "sec_edgar_fetch_filing",
    }


# ---------------------------------------------------------------------------
# Missing User-Agent fast-fail (shared by every verb)
# ---------------------------------------------------------------------------


def test_missing_user_agent_raises() -> None:
    from parsimony_sec_edgar import _user_agent

    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError) as exc:
        _user_agent()
    assert exc.value.env_var == "SEC_EDGAR_USER_AGENT"


def test_blank_user_agent_raises() -> None:
    from parsimony_sec_edgar import _user_agent

    with patch.dict("os.environ", {"SEC_EDGAR_USER_AGENT": "   "}), pytest.raises(UnauthorizedError):
        _user_agent()


def test_find_company_no_user_agent_raises() -> None:
    # The fast-fail fires before any network call, so no HTTP mock is needed.
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError):
        sec_edgar_find_company(identifier="AAPL")


def test_submissions_no_user_agent_raises() -> None:
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError):
        sec_edgar_submissions(cik="320193")


def test_company_facts_no_user_agent_raises() -> None:
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError):
        sec_edgar_company_facts(cik="320193")


def test_fetch_filing_no_user_agent_raises() -> None:
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError):
        sec_edgar_fetch_filing(cik="320193", accession_number="0000320193-24-000123")


# ---------------------------------------------------------------------------
# sec_edgar_find_company
# ---------------------------------------------------------------------------


def test_find_company_returns_match_by_ticker() -> None:
    tickers = [
        {"cik_str": 789, "ticker": "EX", "title": "Example Inc"},
        {"cik_str": 42, "ticker": "OTH", "title": "Other Corp"},
    ]
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar._load_company_tickers", new=MagicMock(return_value=tickers)),
    ):
        result = sec_edgar_find_company(identifier="ex")

    assert result.provenance.source == "sec_edgar_find_company"
    df = result.data
    assert list(df.columns) == ["cik", "title", "ticker"]
    assert df.iloc[0]["ticker"] == "EX"
    assert df.iloc[0]["cik"] == "0000000789"
    assert df.iloc[0]["title"] == "Example Inc"


def test_find_company_matches_by_cik() -> None:
    tickers = [{"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"}]
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar._load_company_tickers", new=MagicMock(return_value=tickers)),
    ):
        result = sec_edgar_find_company(identifier="320193")

    assert result.data.iloc[0]["ticker"] == "AAPL"


def test_find_company_blank_identifier_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_find_company(identifier="   ")


def test_find_company_no_match_raises_empty() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar._load_company_tickers", new=MagicMock(return_value=[])),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_find_company(identifier="NOPE")
    assert exc.value.query_params == {"identifier": "NOPE"}


def test_load_company_tickers_bad_shape_raises_parse() -> None:
    from parsimony_sec_edgar import _load_company_tickers

    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value=["not", "a", "dict"])),
        pytest.raises(ParseError),
    ):
        _load_company_tickers()


# ---------------------------------------------------------------------------
# sec_edgar_submissions
# ---------------------------------------------------------------------------


def _submissions_payload(n: int = 1) -> dict:
    return {
        "filings": {
            "recent": {
                "accessionNumber": [f"000{i:04d}" for i in range(n)],
                "filingDate": ["2024-01-01"] * n,
                "form": ["10-K"] * n,
                "primaryDocument": ["doc.htm"] * n,
            }
        }
    }


def test_submissions_returns_rows() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value=_submissions_payload(3))),
    ):
        result = sec_edgar_submissions(cik="789")

    df = result.data
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["accessionNumber", "filingDate", "form", "primaryDocument"]
    assert df.iloc[0]["form"] == "10-K"
    assert len(df) == 3


def test_submissions_respects_limit() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value=_submissions_payload(50))),
    ):
        result = sec_edgar_submissions(cik="789", limit=5)
    assert len(result.data) == 5


def test_submissions_empty_recent_raises() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value={"filings": {"recent": {}}})),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_submissions(cik="789")
    assert exc.value.query_params == {"cik": "0000000789"}


def test_submissions_non_object_raises_parse() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value=["unexpected"])),
        pytest.raises(ParseError),
    ):
        sec_edgar_submissions(cik="789")


def test_submissions_bad_cik_raises_invalid_param() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_submissions(cik="abc")


# ---------------------------------------------------------------------------
# sec_edgar_company_facts
# ---------------------------------------------------------------------------


def test_company_facts_returns_dict() -> None:
    facts = {"cik": 320193, "entityName": "Apple Inc.", "facts": {"us-gaap": {"Assets": {}}}}
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value=facts)),
    ):
        result = sec_edgar_company_facts(cik="320193")

    assert result.provenance.source == "sec_edgar_company_facts"
    assert result.data["entityName"] == "Apple Inc."
    assert "us-gaap" in result.data["facts"]


def test_company_facts_empty_raises() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch("parsimony_sec_edgar.fetch_json", new=MagicMock(return_value={"cik": 1, "facts": {}})),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_company_facts(cik="1")
    assert exc.value.query_params == {"cik": "0000000001"}


def test_company_facts_bad_cik_raises_invalid_param() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_company_facts(cik="--")


# ---------------------------------------------------------------------------
# sec_edgar_fetch_filing
# ---------------------------------------------------------------------------


@respx.mock
def test_fetch_filing_resolves_primary_document() -> None:
    respx.get("https://data.sec.gov/submissions/CIK0000320193.json").mock(
        return_value=httpx.Response(
            200,
            json={
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000320193-24-000123"],
                        "primaryDocument": ["aapl-20240928.htm"],
                    }
                }
            },
        )
    )
    # The document MUST be fetched from www.sec.gov — data.sec.gov 404s the
    # /Archives path. Mocking only the www host asserts the correct host.
    doc_route = respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm").mock(
        return_value=httpx.Response(200, text="<html>10-K body</html>")
    )

    with patch.dict("os.environ", _UA):
        result = sec_edgar_fetch_filing(cik="320193", accession_number="0000320193-24-000123")

    data = result.data
    assert data["cik"] == "0000320193"
    assert data["document"] == "aapl-20240928.htm"
    assert data["content"] == "<html>10-K body</html>"
    assert doc_route.called, "document was not fetched from www.sec.gov"


@respx.mock
def test_fetch_filing_explicit_document_skips_submissions() -> None:
    sub_route = respx.get(url__startswith="https://data.sec.gov/submissions/").mock(
        return_value=httpx.Response(200, json={})
    )
    doc_route = respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/custom.htm").mock(
        return_value=httpx.Response(200, text="body")
    )

    with patch.dict("os.environ", _UA):
        result = sec_edgar_fetch_filing(
            cik="320193",
            accession_number="0000320193-24-000123",
            document="custom.htm",
        )

    assert result.data["document"] == "custom.htm"
    # With an explicit document, the submissions request is skipped entirely.
    assert not sub_route.called, "submissions should not be fetched when document= is supplied"
    assert doc_route.called


@respx.mock
def test_fetch_filing_falls_back_to_dashed_txt() -> None:
    respx.get("https://data.sec.gov/submissions/CIK0000320193.json").mock(
        return_value=httpx.Response(200, json={"filings": {"recent": {}}})
    )
    # Fallback filename uses the DASHED accession (10-2-6), not the stripped form.
    doc_route = respx.get(
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123.txt"
    ).mock(return_value=httpx.Response(200, text="raw"))

    with patch.dict("os.environ", _UA):
        result = sec_edgar_fetch_filing(cik="320193", accession_number="0000320193-24-000123")

    assert result.data["document"] == "0000320193-24-000123.txt"
    assert doc_route.called


def test_fetch_filing_blank_accession_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_fetch_filing(cik="320193", accession_number="  -- ")


def test_fetch_filing_bad_cik_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_fetch_filing(cik="xyz", accession_number="0000320193-24-000123")
