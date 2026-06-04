"""SEC EDGAR connectors for parsimony.

SEC EDGAR is a **keyless** public source, but SEC's fair-access policy
*requires* every request to carry a ``User-Agent`` header that identifies the
requester (a name and contact email, e.g. ``"Acme Research contact@acme.com"``).
A generic or missing User-Agent gets a ``403``/``429`` from SEC. The header is
supplied via the mandatory ``SEC_EDGAR_USER_AGENT`` environment variable and is
resolved in :func:`_user_agent` before any network call.

The User-Agent is required *infrastructure*, not a secret credential — it is a
header (never a query param, so it is not logged/redacted), so it is **not**
declared via ``secrets=``/``bind()``/``load()``. There is no API key.

Exports :data:`CONNECTORS`:

* ``sec_edgar_find_company`` (``@connector``) — resolve a registrant by ticker
  or CIK from the published ticker map (cik + ticker + title).
* ``sec_edgar_submissions`` (``@connector``) — list recent filings for a CIK.
* ``sec_edgar_company_facts`` (``@connector``) — raw XBRL company-facts dict.
* ``sec_edgar_fetch_filing`` (``@connector``) — one filing document body.
"""

from __future__ import annotations

import os
import re
from typing import Any, cast

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    UnauthorizedError,
)
from parsimony.result import Column, ColumnRole, OutputConfig
from parsimony.transport import HttpClient, map_http_error, map_timeout_error
from parsimony.transport.helpers import fetch_json, make_http_client

__all__ = ["CONNECTORS"]

_DATA_BASE = "https://data.sec.gov"
_WWW_BASE = "https://www.sec.gov"
_ENV_VAR = "SEC_EDGAR_USER_AGENT"

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------
#
# find_company carries a real DATA column (ticker) alongside the KEY/TITLE, so
# it is a plain @connector — NOT an @enumerator (enumerators forbid DATA).
# submissions is a filing *listing* (KEY + DATA, no title) — a @loader-shaped
# frame, but its purpose is a recent-filings listing, not feeding a value store,
# so it stays a plain @connector too.

_FIND_OUTPUT = OutputConfig(
    columns=[
        Column(name="cik", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="ticker", role=ColumnRole.DATA),
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

_SUBMISSIONS_COLUMNS = [c.name for c in _SUBMISSIONS_OUTPUT.columns]


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _user_agent() -> str:
    """Resolve the mandatory SEC User-Agent from the environment.

    SEC's fair-access policy rejects generic/missing User-Agents with 403/429,
    so this fast-fails (before any network call) with a clear, actionable error
    when the operator has not set the env var.
    """
    ua = os.environ.get(_ENV_VAR, "").strip()
    if not ua:
        raise UnauthorizedError(
            "sec_edgar",
            "SEC requires a User-Agent identifying the requester (name + email). "
            f"Set {_ENV_VAR} to a string like 'Acme Research contact@acme.com'.",
            env_var=_ENV_VAR,
        )
    return ua


def _data_client() -> HttpClient:
    """Build a data.sec.gov client carrying the required User-Agent header."""
    return make_http_client(_DATA_BASE, headers={"User-Agent": _user_agent()}, timeout=30.0)


def _www_client() -> HttpClient:
    """Build a www.sec.gov client carrying the required User-Agent header."""
    return make_http_client(_WWW_BASE, headers={"User-Agent": _user_agent()}, timeout=30.0)


def _normalize_cik(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not digits:
        raise InvalidParameterError("sec_edgar", "cik must contain digits")
    return digits.zfill(10)


async def _load_company_tickers() -> list[dict[str, Any]]:
    payload = await fetch_json(
        _www_client(),
        path="/files/company_tickers.json",
        provider="sec_edgar",
        op_name="company_tickers",
    )
    if isinstance(payload, dict):
        return [row for row in payload.values() if isinstance(row, dict)]
    raise ParseError("sec_edgar", "company_tickers.json did not return the expected object shape")


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=_FIND_OUTPUT, tags=["sec_edgar", "tool"])
async def sec_edgar_find_company(identifier: str) -> pd.DataFrame:
    """Find an SEC registrant by ticker symbol or CIK using the published ticker map.

    Returns cik + ticker + company title rows. `identifier` matches a ticker
    exactly (case-insensitive), a CIK exactly (digits, any zero-padding), or a
    substring of the company title.
    """
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
        raise EmptyDataError(
            "sec_edgar",
            message=f"No SEC company found for {identifier!r}",
            query_params={"identifier": query},
        )
    return pd.DataFrame(matches)[[c.name for c in _FIND_OUTPUT.columns]]


@connector(output=_SUBMISSIONS_OUTPUT, tags=["sec_edgar", "tool"])
async def sec_edgar_submissions(cik: str, limit: int = 20) -> pd.DataFrame:
    """List recent SEC filings for a CIK from the EDGAR submissions API.

    Returns accession number + filing date + form type + primary document for
    the most recent filings (newest first), capped at `limit` rows (1-100).
    """
    cik_norm = _normalize_cik(cik)
    limit_clamped = max(1, min(limit, 100))

    payload = await fetch_json(
        _data_client(),
        path=f"/submissions/CIK{cik_norm}.json",
        provider="sec_edgar",
        op_name="submissions",
    )
    if not isinstance(payload, dict):
        raise ParseError("sec_edgar", "submissions response was not a JSON object")

    recent = payload.get("filings", {}).get("recent", {})
    if not isinstance(recent, dict) or not recent:
        raise EmptyDataError(
            "sec_edgar",
            message=f"No submissions returned for CIK {cik_norm}",
            query_params={"cik": cik_norm},
        )

    df = pd.DataFrame(recent)
    keep = [c for c in _SUBMISSIONS_COLUMNS if c in df.columns]
    if not keep or df.empty:
        raise EmptyDataError(
            "sec_edgar",
            message=f"No filing rows for CIK {cik_norm}",
            query_params={"cik": cik_norm},
        )
    return df[keep].head(limit_clamped)


@connector(tags=["sec_edgar", "tool"])
async def sec_edgar_company_facts(cik: str) -> dict[str, Any]:
    """Return the raw XBRL company-facts blob for a CIK.

    Fetches /api/xbrl/companyfacts/CIK{cik}.json — the full set of reported
    financial concepts keyed by taxonomy (us-gaap, dei, …). Returned verbatim
    as a dict for downstream extraction.
    """
    cik_norm = _normalize_cik(cik)
    payload = await fetch_json(
        _data_client(),
        path=f"/api/xbrl/companyfacts/CIK{cik_norm}.json",
        provider="sec_edgar",
        op_name="company_facts",
    )
    if not isinstance(payload, dict) or not payload.get("facts"):
        raise EmptyDataError(
            "sec_edgar",
            message=f"No XBRL company facts returned for CIK {cik_norm}",
            query_params={"cik": cik_norm},
        )
    return cast(dict[str, Any], payload)


@connector(tags=["sec_edgar", "tool"])
async def sec_edgar_fetch_filing(
    cik: str, accession_number: str, document: str | None = None
) -> dict[str, str]:
    """Fetch one SEC filing document body from the EDGAR archives.

    Resolves the primary document for `accession_number` from the company's
    submissions when `document` is omitted, then returns that document's raw
    text/HTML body. Returns a dict of cik + accession_number + document + content.
    Note: a filing body (e.g. a 10-K) can be multiple megabytes.
    """
    cik_norm = _normalize_cik(cik)
    accession = accession_number.strip().replace("-", "")
    if not accession:
        raise InvalidParameterError("sec_edgar", "accession_number is required")

    doc_name = document
    if not doc_name:
        # Resolve the primary document name from the company's submissions
        # (a data.sec.gov JSON call). Skipped entirely when `document` is given.
        submissions = await fetch_json(
            _data_client(),
            path=f"submissions/CIK{cik_norm}.json",
            provider="sec_edgar",
            op_name="submissions",
        )
        recent = submissions.get("filings", {}).get("recent", {}) if isinstance(submissions, dict) else {}
        accession_numbers = recent.get("accessionNumber", []) if isinstance(recent, dict) else []
        primary_docs = recent.get("primaryDocument", []) if isinstance(recent, dict) else []
        for idx, acc in enumerate(accession_numbers):
            if str(acc).replace("-", "") == accession:
                if idx < len(primary_docs):
                    doc_name = str(primary_docs[idx])
                break
    if not doc_name:
        # Full-submission text file: its filename uses the DASHED accession
        # (10-2-6), while the archive directory uses the dash-stripped form.
        dashed = (
            f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
            if len(accession) == 18
            else accession_number.strip()
        )
        doc_name = f"{dashed}.txt"

    # Filing documents are served from www.sec.gov (NOT data.sec.gov, which
    # 404s the /Archives path). The directory uses the dash-stripped accession.
    cik_int = str(int(cik_norm))
    path = f"/Archives/edgar/data/{cik_int}/{accession}/{doc_name}"
    content = await _get_text(_www_client(), path, op_name="fetch_filing")

    return {
        "cik": cik_norm,
        "accession_number": accession_number,
        "document": doc_name,
        "content": content,
    }


async def _get_text(http: HttpClient, path: str, *, op_name: str) -> str:
    """GET *path* and return the raw text body (non-JSON document)."""
    try:
        response = await http.request("GET", path)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="sec_edgar", op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider="sec_edgar", op_name=op_name)
    return response.text


CONNECTORS = Connectors(
    [
        sec_edgar_find_company,
        sec_edgar_submissions,
        sec_edgar_company_facts,
        sec_edgar_fetch_filing,
    ]
)
