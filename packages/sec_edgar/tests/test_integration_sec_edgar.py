"""Live integration tests for parsimony-sec-edgar.

SEC EDGAR is keyless but its fair-access policy *requires* a ``User-Agent``
header identifying the requester (name + email), read from the mandatory
``SEC_EDGAR_USER_AGENT`` env var. These tests ``require_env(...)`` and therefore
**skip cleanly** when the var is unset.

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    SEC_EDGAR_USER_AGENT="You you@example.com" uv run pytest packages/sec_edgar -m integration
"""

from __future__ import annotations

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError
from parsimony_test_support import assert_provenance_shape, require_env

from parsimony_sec_edgar import (
    sec_edgar_balance_sheet,
    sec_edgar_cash_flow,
    sec_edgar_company_concept,
    sec_edgar_company_facts,
    sec_edgar_fetch_filing,
    sec_edgar_find_company,
    sec_edgar_frames,
    sec_edgar_full_text_search,
    sec_edgar_holdings_13f,
    sec_edgar_income_statement,
    sec_edgar_insider_transactions,
    sec_edgar_submissions,
)
from parsimony_sec_edgar.outputs import (
    FINANCIAL_STATEMENT_COLUMNS,
    HOLDINGS_13F_COLUMNS,
    INSIDER_TRANSACTIONS_COLUMNS,
)

pytestmark = pytest.mark.integration

# Apple is the canonical fixture — CIK 0000320193, ticker AAPL.
_APPLE_CIK = "320193"
_APPLE_CIK_PADDED = "0000320193"
# Berkshire Hathaway is the canonical 13F filer — CIK 0001067983.
# (Apple does not file 13F-HR; an operating company has no institutional portfolio.)
_BERKSHIRE_CIK = "1067983"


def test_full_text_search_live() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_full_text_search(query="climate risk", forms="10-K", limit=20)

    assert_provenance_shape(result, expected_source="sec_edgar_full_text_search", required_param_keys=["query"])
    df = result.raw
    assert not df.empty, "full-text search returned no hits"
    assert list(df.columns) == [
        "accession",
        "display_name",
        "form",
        "filing_date",
        "cik",
        "document",
        "period_ending",
        "score",
    ]
    # Real content: every hit must carry an accession, a fetchable document, and the requested form.
    assert df["accession"].astype(str).str.len().gt(0).all(), "blank accession"
    assert df["document"].astype(str).str.len().gt(0).all(), "blank document name"
    assert (df["form"].astype(str).str.startswith("10-K")).any(), "form filter not honored"


def test_find_company_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_find_company(identifier="AAPL")

    assert_provenance_shape(result, expected_source="sec_edgar_find_company", required_param_keys=["identifier"])
    df = result.raw
    assert not df.empty
    assert list(df.columns) == ["cik", "title", "ticker"]
    aapl = df[df["ticker"] == "AAPL"]
    assert not aapl.empty, "AAPL ticker not present in matches"
    assert aapl.iloc[0]["cik"] == _APPLE_CIK_PADDED
    assert "APPLE" in aapl.iloc[0]["title"].upper()


def test_submissions_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_submissions(cik=_APPLE_CIK, limit=10)

    assert_provenance_shape(result, expected_source="sec_edgar_submissions", required_param_keys=["cik"])
    df = result.raw
    assert not df.empty
    assert list(df.columns) == ["accessionNumber", "filingDate", "form", "primaryDocument", "reportDate"]
    assert len(df) <= 10, "limit not respected"
    assert df["accessionNumber"].astype(str).str.len().gt(0).all(), "blank accession number"
    assert df["form"].astype(str).str.len().gt(0).any(), "no real form type"
    assert df["filingDate"].astype(str).str.match(r"\d{4}-\d{2}-\d{2}").any(), "no real filing date"


def test_submissions_include_older_reaches_history() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    # Apple files many 8-Ks/Form-4s, so its recent-1000 window holds only a few
    # 10-Ks. include_older walks the additional pages, surfacing 10-Ks all the
    # way back — the completeness fix the recent-only reader could not reach.
    recent_only = sec_edgar_submissions(cik=_APPLE_CIK, form="10-K", limit=50)
    with_older = sec_edgar_submissions(cik=_APPLE_CIK, form="10-K", limit=50, include_older=True)

    assert len(with_older.raw) > len(recent_only.raw), "include_older surfaced no additional 10-Ks"
    oldest = with_older.raw["filingDate"].astype(str).min()
    assert oldest < "2010-01-01", f"include_older did not reach pre-2010 filings (oldest={oldest})"


def test_company_concept_apple_assets() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_company_concept(cik=_APPLE_CIK, tag="Assets")

    assert_provenance_shape(result, expected_source="sec_edgar_company_concept", required_param_keys=["cik", "tag"])
    df = result.raw
    assert not df.empty, "no Assets facts returned"
    assert {"end", "val", "unit", "form", "filed", "accn"} <= set(df.columns)
    # Real content: values present and large (Apple's total assets are ~$3e11).
    assert df["val"].notna().any(), "no values"
    assert pd.to_numeric(df["val"], errors="coerce").max() > 1e10, "Assets values look wrong"
    assert (df["unit"] == "USD").any(), "expected USD facts"


def test_company_facts_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_company_facts(cik=_APPLE_CIK)

    assert_provenance_shape(result, expected_source="sec_edgar_company_facts", required_param_keys=["cik"])
    facts = result.raw
    assert isinstance(facts, dict)
    assert facts.get("cik") == 320193
    assert "APPLE" in str(facts.get("entityName", "")).upper()
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    assert us_gaap and ("Assets" in us_gaap or len(us_gaap) > 10), "us-gaap facts look unpopulated"


def test_fetch_filing_apple_latest() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    # Resolve a real, recent accession from the live submissions feed rather
    # than hardcoding one (filings roll forward over time).
    subs = sec_edgar_submissions(cik=_APPLE_CIK, form="10-K", limit=5)
    accession = str(subs.raw.iloc[0]["accessionNumber"])

    result = sec_edgar_fetch_filing(cik=_APPLE_CIK, accession_number=accession)

    assert_provenance_shape(
        result, expected_source="sec_edgar_fetch_filing", required_param_keys=["cik", "accession_number"]
    )
    data = result.raw
    assert isinstance(data, dict), f"expected a filing dict, got {type(data)!r}"
    assert data["cik"] == _APPLE_CIK_PADDED
    assert data["document"], "no document name resolved"
    # The primary doc of a 10-K must be a substantial HTML body.
    assert data["document"].lower().endswith((".htm", ".html")), f"primary doc not HTML: {data['document']}"
    assert len(data["content"]) > 1000, f"filing content too short ({len(data['content'])} chars)"


def test_frames_cross_section_live() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_frames(tag="AccountsPayableCurrent", period="CY2019Q1I", unit="USD")

    assert_provenance_shape(result, expected_source="sec_edgar_frames", required_param_keys=["tag", "period"])
    df = result.raw
    assert not df.empty
    assert list(df.columns)[:2] == ["cik", "entityName"]
    # A real cross-section spans thousands of filers with populated values.
    assert len(df) > 1000, f"frame too small ({len(df)} entities)"
    assert df["val"].notna().any(), "no values in frame"
    assert df["entityName"].astype(str).str.len().gt(0).any(), "no entity names"
    assert df["cik"].astype(str).str.fullmatch(r"\d{10}").all(), "cik not 10-digit padded"


def test_find_company_no_match_raises_empty() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    with pytest.raises(EmptyDataError):
        sec_edgar_find_company(identifier="ZZZZZ_NO_SUCH_TICKER_XYZ")


# ---------------------------------------------------------------------------
# edgartools-backed verbs: normalized financial statements (income / balance /
# cash flow), Form 4 insider transactions, and 13F-HR holdings. These exercise
# the synchronous edgartools bridge (_edgar.py) end-to-end against live XBRL.
# ---------------------------------------------------------------------------


def _assert_tidy_statement(df: pd.DataFrame, *, concept_keyword: str) -> None:
    """Shared shape checks for the three financial-statement verbs."""
    assert not df.empty, "statement returned no rows"
    assert list(df.columns) == list(FINANCIAL_STATEMENT_COLUMNS), df.columns.tolist()
    # Tidy long format: every row is a concrete (concept × period × value) datum.
    assert df["concept"].astype(str).str.len().gt(0).all(), "blank concept"
    # Every period must be a real date (YYYY-MM-DD), never a leaked metadata
    # column name like "standard_concept"/"weight"/"balance".
    is_date = df["period"].astype(str).str.fullmatch(r"\d{4}-\d{2}-\d{2}")
    assert is_date.all(), f"non-date period leaked in: {sorted(set(df['period'][~is_date]))}"
    # Every value must be numeric (no text metadata melted into the value axis).
    vals = pd.to_numeric(df["value"], errors="coerce")
    assert vals.notna().all(), "non-numeric values leaked into the value axis"
    # Apple-scale line items run to the hundreds of billions.
    assert vals.abs().max() > 1e9, f"values look too small (max={vals.abs().max()})"
    assert df["concept"].astype(str).str.contains(concept_keyword, case=False).any(), (
        f"expected a concept matching {concept_keyword!r}"
    )


def test_income_statement_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_income_statement(cik=_APPLE_CIK)

    assert_provenance_shape(result, expected_source="sec_edgar_income_statement", required_param_keys=["cik"])
    _assert_tidy_statement(result.raw, concept_keyword="Revenue")


def test_balance_sheet_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_balance_sheet(cik=_APPLE_CIK)

    assert_provenance_shape(result, expected_source="sec_edgar_balance_sheet", required_param_keys=["cik"])
    _assert_tidy_statement(result.raw, concept_keyword="Asset")


def test_cash_flow_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_cash_flow(cik=_APPLE_CIK)

    assert_provenance_shape(result, expected_source="sec_edgar_cash_flow", required_param_keys=["cik"])
    _assert_tidy_statement(result.raw, concept_keyword="Cash")


def test_insider_transactions_apple() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_insider_transactions(cik=_APPLE_CIK, limit=10)

    assert_provenance_shape(result, expected_source="sec_edgar_insider_transactions", required_param_keys=["cik"])
    df = result.raw
    assert not df.empty, "no insider transactions returned"
    assert list(df.columns) == list(INSIDER_TRANSACTIONS_COLUMNS), df.columns.tolist()
    # Form 4 transactions carry a date, a named insider, and a share count.
    assert df["date"].notna().any(), "no transaction dates"
    assert df["insider"].astype(str).str.len().gt(0).any(), "no insider names"
    assert pd.to_numeric(df["shares"], errors="coerce").notna().any(), "no share counts"
    assert (df["ticker"].astype(str) == "AAPL").any(), "AAPL ticker absent from issuer column"


def test_holdings_13f_berkshire() -> None:
    require_env("SEC_EDGAR_USER_AGENT")

    result = sec_edgar_holdings_13f(cik=_BERKSHIRE_CIK)

    assert_provenance_shape(result, expected_source="sec_edgar_holdings_13f", required_param_keys=["cik"])
    df = result.raw
    assert not df.empty, "no 13F holdings returned"
    assert list(df.columns) == list(HOLDINGS_13F_COLUMNS), df.columns.tolist()
    # Berkshire's 13F lists dozens of positions; every row is a real security.
    assert len(df) > 10, f"portfolio too small ({len(df)} positions)"
    assert df["cusip"].astype(str).str.fullmatch(r"[0-9A-Za-z]{9}").all(), "cusip not 9-char"
    vals = pd.to_numeric(df["value"], errors="coerce")
    assert vals.notna().any() and vals.max() > 1e6, "holding values look wrong"
    assert df["issuer"].astype(str).str.len().gt(0).any(), "no issuer names"
