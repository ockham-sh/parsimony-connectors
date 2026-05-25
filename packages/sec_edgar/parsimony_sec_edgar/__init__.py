"""SEC EDGAR source: company lookup, submissions, and XBRL facts via data.sec.gov."""

from __future__ import annotations

import os
import re
from typing import Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector
from parsimony.errors import EmptyDataError, InvalidParameterError, UnauthorizedError
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient, map_http_error, map_timeout_error

_DATA_BASE = "https://data.sec.gov"
_WWW_BASE = "https://www.sec.gov"

_FIND_OUTPUT = OutputConfig(
    columns=[
        Column(name="cik", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="ticker", role=ColumnRole.DATA),
        Column(name="title", role=ColumnRole.TITLE),
    ]
)

_SUBMISSIONS_OUTPUT = OutputConfig(
    columns=[
        Column(name="accessionNumber", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="filingDate", role=ColumnRole.DATA),
        Column(name="form", role=ColumnRole.DATA),
        Column(name="primaryDocument", role=ColumnRole.DATA),
    ]
)


def _user_agent() -> str:
    ua = os.environ.get("SEC_EDGAR_USER_AGENT", "").strip()
    if not ua:
        raise UnauthorizedError("sec_edgar", env_var="SEC_EDGAR_USER_AGENT")
    return ua


def _data_http() -> HttpClient:
    return HttpClient(_DATA_BASE, headers={"User-Agent": _user_agent()}, timeout=30.0)


def _www_http() -> HttpClient:
    return HttpClient(_WWW_BASE, headers={"User-Agent": _user_agent()}, timeout=30.0)


def _normalize_cik(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not digits:
        raise InvalidParameterError("sec_edgar", "cik must contain digits")
    return digits.zfill(10)


async def _get_json(http: HttpClient, path: str, *, op_name: str) -> Any:
    try:
        response = await http.request("GET", path)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="sec_edgar", op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider="sec_edgar", op_name=op_name)
    return response.json()


async def _load_company_tickers() -> list[dict[str, Any]]:
    payload = await _get_json(_www_http(), "/files/company_tickers.json", op_name="company_tickers")
    if isinstance(payload, dict):
        return [row for row in payload.values() if isinstance(row, dict)]
    raise InvalidParameterError("sec_edgar", "Unexpected company_tickers.json shape")


@connector(output=_FIND_OUTPUT, tags=["sec_edgar", "tool"])
async def sec_edgar_find_company(identifier: str) -> pd.DataFrame:
    """Find an SEC registrant by ticker or CIK using the published ticker map."""
    query = identifier.strip()
    if not query:
        raise InvalidParameterError("sec_edgar", "identifier is required")

    rows = await _load_company_tickers()
    query_upper = query.upper()
    query_cik = _normalize_cik(query) if query.isdigit() else None

    matches: list[dict[str, Any]] = []
    for row in rows:
        ticker = str(row.get("ticker", "")).upper()
        title = str(row.get("title", ""))
        cik = _normalize_cik(str(row.get("cik_str", row.get("cik", ""))))
        if query_upper == ticker or (query_cik and query_cik == cik) or query_upper in title.upper():
            matches.append({"cik": cik, "ticker": ticker, "title": title})

    if not matches:
        raise EmptyDataError(provider="sec_edgar", message=f"No SEC company found for {identifier!r}")
    return pd.DataFrame(matches)


@connector(output=_SUBMISSIONS_OUTPUT, tags=["sec_edgar", "tool"])
async def sec_edgar_submissions(cik: str, limit: int = 20) -> pd.DataFrame:
    """List recent SEC filings for a CIK from the submissions API."""
    cik_norm = _normalize_cik(cik)
    payload = await _get_json(_data_http(), f"/submissions/CIK{cik_norm}.json", op_name="submissions")
    recent = payload.get("filings", {}).get("recent", {}) if isinstance(payload, dict) else {}
    if not isinstance(recent, dict) or not recent:
        raise EmptyDataError(provider="sec_edgar", message=f"No submissions returned for CIK {cik_norm}")

    df = pd.DataFrame(recent)
    keep = [c.name for c in _SUBMISSIONS_OUTPUT.columns if c.name in df.columns]
    if not keep:
        raise EmptyDataError(provider="sec_edgar", message=f"No filing rows for CIK {cik_norm}")
    out = df[keep].head(max(1, min(limit, 100)))
    if out.empty:
        raise EmptyDataError(provider="sec_edgar", message=f"No submissions matched for CIK {cik_norm}")
    return out


@connector(tags=["sec_edgar", "tool"])
async def sec_edgar_company_facts(cik: str) -> dict[str, Any]:
    """Return raw XBRL company facts for a CIK."""
    cik_norm = _normalize_cik(cik)
    return await _get_json(_data_http(), f"/api/xbrl/companyfacts/CIK{cik_norm}.json", op_name="company_facts")


@connector(tags=["sec_edgar", "tool"])
async def sec_edgar_fetch_filing(cik: str, accession_number: str, document: str | None = None) -> dict[str, str]:
    """Fetch one SEC filing document body from data.sec.gov."""
    cik_norm = _normalize_cik(cik)
    accession = accession_number.strip().replace("-", "")
    if not accession:
        raise InvalidParameterError("sec_edgar", "accession_number is required")

    submissions = await _get_json(_data_http(), f"/submissions/CIK{cik_norm}.json", op_name="submissions")
    recent = submissions.get("filings", {}).get("recent", {}) if isinstance(submissions, dict) else {}
    accession_numbers = recent.get("accessionNumber", []) if isinstance(recent, dict) else []
    primary_docs = recent.get("primaryDocument", []) if isinstance(recent, dict) else []

    doc_name = document
    if not doc_name:
        for idx, acc in enumerate(accession_numbers):
            if str(acc).replace("-", "") == accession:
                if idx < len(primary_docs):
                    doc_name = str(primary_docs[idx])
                break
    if not doc_name:
        doc_name = f"{accession}.txt"

    cik_int = str(int(cik_norm))
    path = f"/Archives/edgar/data/{cik_int}/{accession}/{doc_name}"
    http = _data_http()
    try:
        response = await http.request("GET", path)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="sec_edgar", op_name="fetch_filing")
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider="sec_edgar", op_name="fetch_filing")
    return {"cik": cik_norm, "accession_number": accession_number, "document": doc_name, "content": response.text}


CONNECTORS = Connectors(
    [
        sec_edgar_find_company,
        sec_edgar_submissions,
        sec_edgar_fetch_filing,
        sec_edgar_company_facts,
    ]
)

__all__ = ["CONNECTORS"]
