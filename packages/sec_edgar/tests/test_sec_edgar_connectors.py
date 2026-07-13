"""Offline tests for SEC EDGAR connectors.

Every verb's happy path, the empty/parse/invalid-param guards, the
``dateRange=custom`` full-text-search date rule, the primary-document picker, and
the missing-User-Agent fast-fail across ALL twelve verbs — without touching the
network (HTTP is mocked / seams patched). Live behaviour is covered in
``test_integration_sec_edgar.py``.

Patches target the real module locations (``…connectors.search``/``.filings``/
``.xbrl``/``._edgar``), not the facade, so they take effect where the symbols
are used.
"""

from __future__ import annotations

from typing import Any
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

import parsimony_sec_edgar._edgar as edgar_mod
import parsimony_sec_edgar._http as http_mod
import parsimony_sec_edgar.connectors.filings as filings_mod
import parsimony_sec_edgar.connectors.search as search_mod
import parsimony_sec_edgar.connectors.xbrl as xbrl_mod
from parsimony_sec_edgar import (
    CONNECTORS,
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

_UA = {"SEC_EDGAR_USER_AGENT": "TestCo test@example.com"}


# ---------------------------------------------------------------------------
# Collection / shape
# ---------------------------------------------------------------------------


def test_connectors_collection_has_twelve_connectors() -> None:
    assert len(CONNECTORS) == 12
    assert set(CONNECTORS.names()) == {
        "sec_edgar_full_text_search",
        "sec_edgar_find_company",
        "sec_edgar_submissions",
        "sec_edgar_fetch_filing",
        "sec_edgar_company_concept",
        "sec_edgar_company_facts",
        "sec_edgar_frames",
        "sec_edgar_income_statement",
        "sec_edgar_balance_sheet",
        "sec_edgar_cash_flow",
        "sec_edgar_insider_transactions",
        "sec_edgar_holdings_13f",
    }


# ---------------------------------------------------------------------------
# Missing User-Agent fast-fail (shared by every verb) — count-guarded
# ---------------------------------------------------------------------------

_VERB_CALLS: list[tuple[Any, dict[str, Any]]] = [
    (sec_edgar_full_text_search, {"query": "apple"}),
    (sec_edgar_find_company, {"identifier": "AAPL"}),
    (sec_edgar_submissions, {"cik": "320193"}),
    (sec_edgar_fetch_filing, {"cik": "320193", "accession_number": "0000320193-24-000123"}),
    (sec_edgar_company_concept, {"cik": "320193", "tag": "Assets"}),
    (sec_edgar_company_facts, {"cik": "320193"}),
    (sec_edgar_frames, {"tag": "Assets", "period": "CY2023Q1I"}),
    (sec_edgar_income_statement, {"cik": "320193"}),
    (sec_edgar_balance_sheet, {"cik": "320193"}),
    (sec_edgar_cash_flow, {"cik": "320193"}),
    (sec_edgar_insider_transactions, {"cik": "320193"}),
    (sec_edgar_holdings_13f, {"cik": "320193"}),
]


def test_verb_call_table_covers_every_connector() -> None:
    # If a verb is added/removed, this list (and the fast-fail coverage below)
    # must change in lockstep with CONNECTORS.
    assert len(_VERB_CALLS) == len(CONNECTORS)


def test_user_agent_missing_raises() -> None:
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError) as exc:
        http_mod.user_agent()
    assert exc.value.env_var == "SEC_EDGAR_USER_AGENT"


def test_user_agent_blank_raises() -> None:
    with patch.dict("os.environ", {"SEC_EDGAR_USER_AGENT": "   "}), pytest.raises(UnauthorizedError):
        http_mod.user_agent()


@pytest.mark.parametrize("verb,kwargs", _VERB_CALLS)
def test_every_verb_fast_fails_without_user_agent(verb: Any, kwargs: dict[str, Any]) -> None:
    # The fast-fail fires when the client is built — before any network call.
    with patch.dict("os.environ", {}, clear=True), pytest.raises(UnauthorizedError):
        verb(**kwargs)


# ---------------------------------------------------------------------------
# sec_edgar_full_text_search
# ---------------------------------------------------------------------------


def _fts_payload(n: int = 2) -> dict:
    return {
        "hits": {
            "total": {"value": n, "relation": "eq"},
            "hits": [
                {
                    "_id": f"0000320193-24-00012{i}:doc{i}.htm",
                    "_score": 9.0 - i,
                    "_source": {
                        "adsh": f"0000320193-24-00012{i}",
                        "display_names": [f"Company {i} (TICK{i}) (CIK 000032019{i})"],
                        "form": "10-K",
                        "file_date": f"2024-02-0{i + 1}",
                        "ciks": [f"000032019{i}"],
                        "period_ending": "2023-12-31",
                    },
                }
                for i in range(n)
            ],
        }
    }


def test_full_text_search_returns_hits() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(search_mod, "fetch_json", new=MagicMock(return_value=_fts_payload(2))),
    ):
        result = sec_edgar_full_text_search(query="climate risk")

    assert result.provenance.source == "sec_edgar_full_text_search"
    df = result.raw
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
    assert df.iloc[0]["accession"] == "0000320193-24-000120"
    assert df.iloc[0]["document"] == "doc0.htm"
    assert df.iloc[0]["form"] == "10-K"


def test_full_text_search_dates_set_daterange_custom() -> None:
    mock = MagicMock(return_value=_fts_payload(1))
    with patch.dict("os.environ", _UA), patch.object(search_mod, "fetch_json", new=mock):
        sec_edgar_full_text_search(query="apple", forms="10-K", start_date="2023-01-01", end_date="2023-12-31")
    params = mock.call_args.kwargs["params"]
    # The API 500s on startdt/enddt without dateRange=custom (verified live).
    assert params["dateRange"] == "custom"
    assert params["startdt"] == "2023-01-01" and params["enddt"] == "2023-12-31"
    assert params["forms"] == "10-K"


def test_full_text_search_blank_query_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_full_text_search(query="   ")


def test_full_text_search_one_date_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_full_text_search(query="apple", start_date="2023-01-01")


def test_full_text_search_no_hits_raises_empty() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(search_mod, "fetch_json", new=MagicMock(return_value={"hits": {"hits": []}})),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_full_text_search(query="zzzz")
    assert exc.value.query_params == {"query": "zzzz"}


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
        patch.object(search_mod, "_load_company_tickers", new=MagicMock(return_value=tickers)),
    ):
        result = sec_edgar_find_company(identifier="ex")

    assert result.provenance.source == "sec_edgar_find_company"
    df = result.raw
    assert list(df.columns) == ["cik", "title", "ticker"]
    assert df.iloc[0]["ticker"] == "EX"
    assert df.iloc[0]["cik"] == "0000000789"
    assert df.iloc[0]["title"] == "Example Inc"


def test_find_company_matches_by_cik() -> None:
    tickers = [{"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"}]
    with (
        patch.dict("os.environ", _UA),
        patch.object(search_mod, "_load_company_tickers", new=MagicMock(return_value=tickers)),
    ):
        result = sec_edgar_find_company(identifier="320193")
    assert result.raw.iloc[0]["ticker"] == "AAPL"


def test_find_company_blank_identifier_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_find_company(identifier="   ")


def test_find_company_no_match_raises_empty() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(search_mod, "_load_company_tickers", new=MagicMock(return_value=[])),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_find_company(identifier="NOPE")
    assert exc.value.query_params == {"identifier": "NOPE"}


def test_load_company_tickers_bad_shape_raises_parse() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(search_mod, "fetch_json", new=MagicMock(return_value=["not", "a", "dict"])),
        pytest.raises(ParseError),
    ):
        search_mod._load_company_tickers()


# ---------------------------------------------------------------------------
# sec_edgar_submissions
# ---------------------------------------------------------------------------


def _submissions_payload(n: int = 1, files: list[dict] | None = None) -> dict:
    return {
        "filings": {
            "recent": {
                "accessionNumber": [f"000{i:04d}" for i in range(n)],
                "filingDate": [f"2024-01-{(i % 28) + 1:02d}" for i in range(n)],
                "reportDate": ["2023-12-31"] * n,
                "form": ["10-K"] * n,
                "primaryDocument": ["doc.htm"] * n,
            },
            "files": files or [],
        }
    }


def _older_page() -> dict:
    return {
        "accessionNumber": ["0000999-01-000001"],
        "filingDate": ["2001-05-07"],
        "reportDate": [""],
        "form": ["10-K"],
        "primaryDocument": ["old.htm"],
    }


def test_submissions_returns_rows() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(filings_mod, "fetch_json", new=MagicMock(return_value=_submissions_payload(3))),
    ):
        result = sec_edgar_submissions(cik="789")
    df = result.raw
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["accessionNumber", "filingDate", "form", "primaryDocument", "reportDate"]
    assert df.iloc[0]["form"] == "10-K"
    assert len(df) == 3


def test_submissions_respects_limit() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(filings_mod, "fetch_json", new=MagicMock(return_value=_submissions_payload(50))),
    ):
        result = sec_edgar_submissions(cik="789", limit=5)
    assert len(result.raw) == 5


def test_submissions_form_filter() -> None:
    payload = _submissions_payload(3)
    payload["filings"]["recent"]["form"] = ["10-K", "8-K", "10-Q"]
    with (
        patch.dict("os.environ", _UA),
        patch.object(filings_mod, "fetch_json", new=MagicMock(return_value=payload)),
    ):
        result = sec_edgar_submissions(cik="789", form="8-k")
    assert list(result.raw["form"]) == ["8-K"]


def test_submissions_include_older_walks_pages() -> None:
    recent = _submissions_payload(2, files=[{"name": "CIK0000000789-submissions-001.json"}])
    pages = [recent, _older_page()]
    call_count = 0

    def _side_effect(*args, **kwargs):
        nonlocal call_count
        result = pages[call_count]
        call_count += 1
        return result

    with patch.dict("os.environ", _UA), patch.object(filings_mod, "fetch_json", side_effect=_side_effect):
        result = sec_edgar_submissions(cik="789", limit=10, include_older=True)
    # recent (2) + the one older page (1) = 3 rows; the older filing is reachable.
    assert len(result.raw) == 3
    assert "0000999-01-000001" in set(result.raw["accessionNumber"])
    assert call_count == 2  # recent + 1 additional page


def test_submissions_default_skips_older_pages() -> None:
    recent = _submissions_payload(2, files=[{"name": "CIK0000000789-submissions-001.json"}])
    call_count = 0

    def _side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return recent

    with patch.dict("os.environ", _UA), patch.object(filings_mod, "fetch_json", side_effect=_side_effect):
        result = sec_edgar_submissions(cik="789")
    assert len(result.raw) == 2
    assert call_count == 1  # only recent fetched when include_older is False


def test_submissions_empty_recent_raises() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(filings_mod, "fetch_json", new=MagicMock(return_value={"filings": {"recent": {}}})),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_submissions(cik="789")
    assert exc.value.query_params == {"cik": "0000000789"}


def test_submissions_non_object_raises_parse() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(filings_mod, "fetch_json", new=MagicMock(return_value=["unexpected"])),
        pytest.raises(ParseError),
    ):
        sec_edgar_submissions(cik="789")


def test_submissions_bad_cik_raises_invalid_param() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_submissions(cik="abc")


# ---------------------------------------------------------------------------
# sec_edgar_fetch_filing (+ the primary-document picker)
# ---------------------------------------------------------------------------


def test_pick_primary_document_prefers_largest_html() -> None:
    items = [
        {"name": "0000320193-24-000123-index.html", "size": "1000"},
        {"name": "R1.htm", "size": "5000"},
        {"name": "aapl-20240928.htm", "size": "900000"},
        {"name": "exhibit99.htm", "size": "2000"},
        {"name": "aapl-20240928.xsd", "size": "4000"},
        {"name": "MetaLinks.json", "size": "8000"},
    ]
    assert filings_mod._pick_primary_document(items) == "aapl-20240928.htm"


def test_pick_primary_document_form4_xml() -> None:
    items = [
        {"name": "0001140361-26-023363-index.html", "size": ""},
        {"name": "form4.xml", "size": "5404"},
    ]
    assert filings_mod._pick_primary_document(items) == "form4.xml"


@respx.mock
def test_fetch_filing_resolves_via_index_json() -> None:
    respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/index.json").mock(
        return_value=httpx.Response(
            200,
            json={
                "directory": {
                    "item": [
                        {"name": "0000320193-24-000123-index.html", "size": "1000"},
                        {"name": "aapl-20240928.htm", "size": "900000"},
                    ]
                }
            },
        )
    )
    doc_route = respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20240928.htm").mock(
        return_value=httpx.Response(200, text="<html>10-K body</html>")
    )

    with patch.dict("os.environ", _UA):
        result = sec_edgar_fetch_filing(cik="320193", accession_number="0000320193-24-000123")

    data = result.raw
    assert data["cik"] == "0000320193"
    assert data["document"] == "aapl-20240928.htm"
    assert data["content"] == "<html>10-K body</html>"
    assert doc_route.called


@respx.mock
def test_fetch_filing_explicit_document_skips_index() -> None:
    index_route = respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/index.json").mock(
        return_value=httpx.Response(200, json={"directory": {"item": []}})
    )
    doc_route = respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/custom.htm").mock(
        return_value=httpx.Response(200, text="body")
    )

    with patch.dict("os.environ", _UA):
        result = sec_edgar_fetch_filing(cik="320193", accession_number="0000320193-24-000123", document="custom.htm")

    assert result.raw["document"] == "custom.htm"
    assert doc_route.called
    # With an explicit document, index.json must NOT be hit.
    assert not index_route.called


@respx.mock
def test_fetch_filing_falls_back_to_dashed_txt() -> None:
    # index.json returns empty items list → no primary doc → fall back to .txt
    respx.get("https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/index.json").mock(
        return_value=httpx.Response(200, json={"directory": {"item": []}})
    )
    # Fallback filename uses the DASHED accession (10-2-6), not the stripped form.
    doc_route = respx.get(
        "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/0000320193-24-000123.txt"
    ).mock(return_value=httpx.Response(200, text="raw"))

    with patch.dict("os.environ", _UA):
        result = sec_edgar_fetch_filing(cik="320193", accession_number="0000320193-24-000123")

    assert result.raw["document"] == "0000320193-24-000123.txt"
    assert doc_route.called


def test_fetch_filing_blank_accession_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_fetch_filing(cik="320193", accession_number="  -- ")


def test_fetch_filing_bad_cik_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_fetch_filing(cik="xyz", accession_number="0000320193-24-000123")


# ---------------------------------------------------------------------------
# sec_edgar_company_concept
# ---------------------------------------------------------------------------


def _concept_payload() -> dict:
    return {
        "cik": 320193,
        "taxonomy": "us-gaap",
        "tag": "Revenues",
        "label": "Revenues",
        "entityName": "Apple Inc.",
        "units": {
            "USD": [
                {
                    "end": "2023-09-30",
                    "val": 383285000000,
                    "accn": "a-1",
                    "fy": 2023,
                    "fp": "FY",
                    "form": "10-K",
                    "filed": "2023-11-03",
                    "start": "2022-10-01",
                },
                {
                    "end": "2022-09-24",
                    "val": 394328000000,
                    "accn": "a-2",
                    "fy": 2022,
                    "fp": "FY",
                    "form": "10-K",
                    "filed": "2022-10-28",
                    "start": "2021-09-26",
                },
            ]
        },
    }


def test_company_concept_returns_long_frame() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value=_concept_payload())),
    ):
        result = sec_edgar_company_concept(cik="320193", tag="Revenues")
    df = result.raw
    assert result.provenance.source == "sec_edgar_company_concept"
    assert {"end", "val", "unit", "fy", "fp", "form", "filed", "accn", "start"} <= set(df.columns)
    assert df["val"].notna().all()
    assert set(df["unit"]) == {"USD"}
    assert len(df) == 2


def test_company_concept_unit_filter() -> None:
    payload = _concept_payload()
    payload["units"]["CAD"] = [
        {"end": "2023-09-30", "val": 1, "accn": "c-1", "fy": 2023, "fp": "FY", "form": "10-K", "filed": "2023-11-03"}
    ]
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value=payload)),
    ):
        result = sec_edgar_company_concept(cik="320193", tag="Revenues", unit="CAD")
    assert set(result.raw["unit"]) == {"CAD"}
    assert len(result.raw) == 1


def test_company_concept_empty_units_raises() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value={"cik": 1, "units": {}})),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_company_concept(cik="1", tag="Assets")


def test_company_concept_blank_tag_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_company_concept(cik="320193", tag="  ")


def test_company_concept_bad_cik_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_company_concept(cik="--", tag="Assets")


# ---------------------------------------------------------------------------
# sec_edgar_company_facts
# ---------------------------------------------------------------------------


def test_company_facts_returns_dict() -> None:
    facts = {"cik": 320193, "entityName": "Apple Inc.", "facts": {"us-gaap": {"Assets": {}}}}
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value=facts)),
    ):
        result = sec_edgar_company_facts(cik="320193")
    assert result.provenance.source == "sec_edgar_company_facts"
    assert result.raw["entityName"] == "Apple Inc."
    assert "us-gaap" in result.raw["facts"]


def test_company_facts_empty_raises() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value={"cik": 1, "facts": {}})),
        pytest.raises(EmptyDataError) as exc,
    ):
        sec_edgar_company_facts(cik="1")
    assert exc.value.query_params == {"cik": "0000000001"}


def test_company_facts_bad_cik_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_company_facts(cik="--")


# ---------------------------------------------------------------------------
# sec_edgar_frames
# ---------------------------------------------------------------------------


def _frames_payload() -> dict:
    return {
        "taxonomy": "us-gaap",
        "tag": "AccountsPayableCurrent",
        "ccp": "CY2019Q1I",
        "uom": "USD",
        "pts": 2,
        "data": [
            {
                "accn": "a-1",
                "cik": 1750,
                "entityName": "AAR CORP.",
                "loc": "US-IL",
                "end": "2019-02-28",
                "val": 218600000,
            },
            {
                "accn": "a-2",
                "cik": 320193,
                "entityName": "Apple Inc.",
                "loc": "US-CA",
                "end": "2019-03-30",
                "val": 37000000000,
            },
        ],
    }


def test_frames_returns_cross_section() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value=_frames_payload())),
    ):
        result = sec_edgar_frames(tag="AccountsPayableCurrent", period="CY2019Q1I")
    df = result.raw
    assert result.provenance.source == "sec_edgar_frames"
    assert list(df.columns)[:2] == ["cik", "entityName"]
    assert df["val"].notna().all()
    # CIK normalized to 10-digit zero-padded string.
    assert df.iloc[0]["cik"] == "0000001750"
    assert df.iloc[1]["entityName"] == "Apple Inc."


def test_frames_bad_period_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_frames(tag="Assets", period="2019Q1")


def test_frames_blank_tag_raises() -> None:
    with patch.dict("os.environ", _UA), pytest.raises(InvalidParameterError):
        sec_edgar_frames(tag="  ", period="CY2019Q1I")


def test_frames_empty_data_raises() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value={"data": []})),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_frames(tag="Assets", period="CY2019Q1I")


def test_frames_non_list_data_raises_parse() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(xbrl_mod, "fetch_json", new=MagicMock(return_value={"data": "nope"})),
        pytest.raises(ParseError),
    ):
        sec_edgar_frames(tag="Assets", period="CY2019Q1I")


# ---------------------------------------------------------------------------
# sec_edgar_income_statement / sec_edgar_balance_sheet / sec_edgar_cash_flow
# (edgartools bridge — patched at _edgar module level)
# ---------------------------------------------------------------------------


def _financial_statement_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "concept": ["Revenues", "Revenues", "NetIncomeLoss", "NetIncomeLoss"],
            "label": ["Revenue", "Revenue", "Net Income", "Net Income"],
            "period": ["2023-09-30", "2022-09-24", "2023-09-30", "2022-09-24"],
            "value": [383285000000, 394328000000, 96995000000, 99803000000],
        }
    )


def test_income_statement_returns_tidy_frame() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_financials", return_value=_financial_statement_df()),
    ):
        result = sec_edgar_income_statement(cik="320193")
    assert result.provenance.source == "sec_edgar_income_statement"
    df = result.raw
    assert list(df.columns) == ["concept", "label", "period", "value"]
    assert len(df) == 4
    assert df.iloc[0]["concept"] == "Revenues"


def test_income_statement_empty_raises() -> None:
    from parsimony.errors import EmptyDataError as _E

    err = _E("sec_edgar", message="no data", query_params={"cik": "0000320193"})
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_financials", side_effect=err),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_income_statement(cik="320193")


def test_balance_sheet_returns_tidy_frame() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_financials", return_value=_financial_statement_df()),
    ):
        result = sec_edgar_balance_sheet(cik="320193")
    assert result.provenance.source == "sec_edgar_balance_sheet"
    df = result.raw
    assert list(df.columns) == ["concept", "label", "period", "value"]
    assert len(df) == 4


def test_balance_sheet_empty_raises() -> None:
    from parsimony.errors import EmptyDataError as _E

    err = _E("sec_edgar", message="no data", query_params={"cik": "0000320193"})
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_financials", side_effect=err),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_balance_sheet(cik="320193")


def test_cash_flow_returns_tidy_frame() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_financials", return_value=_financial_statement_df()),
    ):
        result = sec_edgar_cash_flow(cik="320193")
    assert result.provenance.source == "sec_edgar_cash_flow"
    df = result.raw
    assert list(df.columns) == ["concept", "label", "period", "value"]
    assert len(df) == 4


def test_cash_flow_empty_raises() -> None:
    from parsimony.errors import EmptyDataError as _E

    err = _E("sec_edgar", message="no data", query_params={"cik": "0000320193"})
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_financials", side_effect=err),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_cash_flow(cik="320193")


# ---------------------------------------------------------------------------
# sec_edgar_insider_transactions (edgartools bridge)
# ---------------------------------------------------------------------------


def _insider_transactions_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
            "issuer": ["Apple Inc.", "Apple Inc."],
            "ticker": ["AAPL", "AAPL"],
            "insider": ["Cook Timothy D", "Cook Timothy D"],
            "position": ["Chief Executive Officer", "Chief Executive Officer"],
            "transaction_type": ["Sale", "Sale"],
            "code": ["S", "S"],
            "shares": [100000.0, 50000.0],
            "price": [182.5, 185.0],
            "value": [18250000.0, 9250000.0],
            "remaining_shares": [3500000.0, 3450000.0],
        }
    )


def test_insider_transactions_returns_frame() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_insider_transactions", return_value=_insider_transactions_df()),
    ):
        result = sec_edgar_insider_transactions(cik="320193")
    assert result.provenance.source == "sec_edgar_insider_transactions"
    df = result.raw
    assert "insider" in df.columns
    assert "transaction_type" in df.columns
    assert "shares" in df.columns
    assert len(df) == 2
    assert df.iloc[0]["ticker"] == "AAPL"


def test_insider_transactions_limit_forwarded() -> None:
    mock = MagicMock(return_value=_insider_transactions_df())
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_insider_transactions", mock),
    ):
        sec_edgar_insider_transactions(cik="320193", limit=5)
    # limit is the second positional arg
    assert mock.call_args[0][1] == 5


def test_insider_transactions_empty_raises() -> None:
    from parsimony.errors import EmptyDataError as _E

    err = _E("sec_edgar", message="no Form 4", query_params={"cik": "0000320193"})
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_insider_transactions", side_effect=err),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_insider_transactions(cik="320193")


# ---------------------------------------------------------------------------
# sec_edgar_holdings_13f (edgartools bridge)
# ---------------------------------------------------------------------------


def _holdings_13f_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cusip": ["037833100", "594918104"],
            "issuer": ["Apple Inc.", "Microsoft Corp"],
            "ticker": ["AAPL", "MSFT"],
            "security_class": ["COM", "COM"],
            "security_type": ["SH", "SH"],
            "put_call": [None, None],
            "shares": [5000000.0, 3000000.0],
            "value": [912500000.0, 1125000000.0],
        }
    )


def test_holdings_13f_returns_frame() -> None:
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_holdings_13f", return_value=_holdings_13f_df()),
    ):
        result = sec_edgar_holdings_13f(cik="1067983")
    assert result.provenance.source == "sec_edgar_holdings_13f"
    df = result.raw
    assert list(df.columns) == [
        "cusip",
        "issuer",
        "ticker",
        "security_class",
        "security_type",
        "put_call",
        "shares",
        "value",
    ]
    assert len(df) == 2
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["cusip"] == "037833100"


def test_holdings_13f_missing_put_call_fills_nan() -> None:
    # put_call is conditional (options only); if not present in source frame,
    # reindex should fill it with NaN rather than raise.
    df_no_pc = _holdings_13f_df().drop(columns=["put_call"])
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_holdings_13f", return_value=df_no_pc),
    ):
        result = sec_edgar_holdings_13f(cik="1067983")
    assert "put_call" in result.raw.columns
    assert result.raw["put_call"].isna().all()


def test_holdings_13f_empty_raises() -> None:
    from parsimony.errors import EmptyDataError as _E

    err = _E("sec_edgar", message="no 13F", query_params={"cik": "0001067983"})
    with (
        patch.dict("os.environ", _UA),
        patch.object(edgar_mod, "_sync_get_holdings_13f", side_effect=err),
        pytest.raises(EmptyDataError),
    ):
        sec_edgar_holdings_13f(cik="1067983")
