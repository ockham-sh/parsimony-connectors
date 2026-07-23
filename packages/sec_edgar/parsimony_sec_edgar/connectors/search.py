"""SEC EDGAR discovery verbs: full-text search (the native search) + ticker lookup.

EDGAR full-text search (``efts.sec.gov``) is the authoritative discovery surface
— it searches the *content* of every filing since 2001 across all ~800k+ filers.
The ticker map (``sec_edgar_find_company``) is the complementary fast path: an
exact ticker/CIK/name lookup, but only over the ~10.4k exchange-listed issuers
that carry a ticker.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_sec_edgar._http import PROVIDER, efts_client, normalize_cik, www_client
from parsimony_sec_edgar.outputs import (
    FIND_COMPANY_COLUMNS,
    FIND_COMPANY_OUTPUT,
    FULL_TEXT_SEARCH_COLUMNS,
    FULL_TEXT_SEARCH_OUTPUT,
)

_FTS_PATH = "/LATEST/search-index"
_FTS_PAGE = 100  # efts returns up to 100 hits per page; `offset` pages beyond.


@connector(output=FULL_TEXT_SEARCH_OUTPUT, tags=["sec_edgar", "tool"], requires=("SEC_EDGAR_USER_AGENT",))
def sec_edgar_full_text_search(
    query: str,
    forms: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    ciks: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> pd.DataFrame:
    """Search the full text of SEC EDGAR filings (2001 to present) across all registrants.

    Returns one row per matching filing document: accession + company display
    name + form + filing date + cik + document + period + relevance score. Pass
    the returned `cik`, `accession`, and `document` straight to
    `sec_edgar_fetch_filing` to read a hit. `forms` restricts by form type
    (e.g. "10-K" or "10-K,8-K"); `start_date`/`end_date` (YYYY-MM-DD) bound the
    filing date and must be given together; `ciks` restricts to a registrant;
    `offset` pages through results (100 per page). Hits are ranked by
    RELEVANCE, not date (EDGAR's full-text API has no date sort) — for "most
    recent", bound the dates and sort the returned rows by `filing_date`.
    """
    q = query.strip()
    if not q:
        raise InvalidParameterError(PROVIDER, "query is required")
    if (start_date is None) != (end_date is None):
        raise InvalidParameterError(PROVIDER, "provide both start_date and end_date (or neither)")

    params: dict[str, Any] = {"q": q, "forms": forms or None, "ciks": ciks or None}
    if start_date and end_date:
        # SEC's full-text API returns HTTP 500 if startdt/enddt are sent without
        # dateRange=custom (verified live).
        params["dateRange"] = "custom"
        params["startdt"] = start_date
        params["enddt"] = end_date
    if offset > 0:
        params["from"] = offset

    payload = fetch_json(efts_client(), path=_FTS_PATH, params=params, op_name="full_text_search")
    if not isinstance(payload, dict):
        raise ParseError(PROVIDER, "full-text search response was not a JSON object")

    hits = payload.get("hits", {}).get("hits", []) if isinstance(payload.get("hits"), dict) else []
    if not hits:
        raise EmptyDataError(PROVIDER, message=f"No filings matched {query!r}", query_params={"query": q})

    rows: list[dict[str, Any]] = []
    for hit in hits[: max(1, min(limit, _FTS_PAGE))]:
        src = hit.get("_source", {}) if isinstance(hit, dict) else {}
        hit_id = str(hit.get("_id", ""))
        document = hit_id.split(":", 1)[1] if ":" in hit_id else ""
        rows.append(
            {
                "accession": src.get("adsh") or (hit_id.split(":", 1)[0] if hit_id else ""),
                "display_name": (src.get("display_names") or [""])[0],
                "form": src.get("form", ""),
                "filing_date": src.get("file_date") or None,
                "cik": (src.get("ciks") or [""])[0],
                "document": document,
                "period_ending": src.get("period_ending") or None,
                "score": hit.get("_score"),
            }
        )
    return pd.DataFrame(rows)[list(FULL_TEXT_SEARCH_COLUMNS)]


def _load_company_tickers() -> list[dict[str, Any]]:
    payload = fetch_json(
        www_client(),
        path="/files/company_tickers.json",
        op_name="company_tickers",
    )
    if isinstance(payload, dict):
        return [row for row in payload.values() if isinstance(row, dict)]
    raise ParseError(PROVIDER, "company_tickers.json did not return the expected object shape")


@connector(output=FIND_COMPANY_OUTPUT, tags=["sec_edgar", "tool"], requires=("SEC_EDGAR_USER_AGENT",))
def sec_edgar_find_company(identifier: str) -> pd.DataFrame:
    """Find an SEC registrant by ticker symbol or CIK using the published ticker map.

    Returns cik + ticker + company title rows. `identifier` matches a ticker
    exactly (case-insensitive), a CIK exactly (digits, any zero-padding), or a
    substring of the company title. Covers the ~10.4k exchange-listed issuers
    that carry a ticker; for everything else (funds, foreign filers, individual
    filers) use `sec_edgar_full_text_search`.
    """
    query = identifier.strip()
    if not query:
        raise InvalidParameterError(PROVIDER, "identifier is required")

    rows = _load_company_tickers()
    query_upper = query.upper()
    query_cik = normalize_cik(query) if query.isdigit() else None

    matches: list[dict[str, Any]] = []
    for row in rows:
        ticker = str(row.get("ticker", "")).upper()
        title = str(row.get("title", ""))
        cik = normalize_cik(str(row.get("cik_str", row.get("cik", ""))))
        if query_upper == ticker or (query_cik and query_cik == cik) or query_upper in title.upper():
            matches.append({"cik": cik, "ticker": ticker, "title": title})

    if not matches:
        raise EmptyDataError(
            PROVIDER,
            message=f"No SEC company found for {identifier!r}",
            query_params={"identifier": query},
        )
    return pd.DataFrame(matches)[list(FIND_COMPANY_COLUMNS)]
