"""SEC EDGAR filing verbs: list a filer's filings, and fetch one document body.

``sec_edgar_submissions`` lists a registrant's filings newest-first. The EDGAR
submissions JSON keeps only the most-recent ≥1000 filings inline
(``filings.recent``); older ones are paged into ``filings.files[]`` additional
JSON files. ``include_older=True`` walks those pages so a prolific filer's full
history is reachable (JPMorgan has ~158k filings across 67 pages).

``sec_edgar_fetch_filing`` resolves the primary document via the accession
folder's ``index.json`` (which works for **any** accession, old or new, and
returns the raw document rather than an XSL-rendered viewer path), then fetches
the body from ``www.sec.gov`` (``data.sec.gov`` 404s the ``/Archives`` path).
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_sec_edgar._http import (
    PROVIDER,
    data_client,
    get_text,
    normalize_cik,
    www_client,
)
from parsimony_sec_edgar.outputs import SUBMISSIONS_COLUMNS, SUBMISSIONS_OUTPUT


def _recent_frame(columnar: dict[str, Any]) -> pd.DataFrame:
    """Build a DataFrame from a submissions columnar block, keeping known columns."""
    if not isinstance(columnar, dict) or not columnar:
        return pd.DataFrame()
    df = pd.DataFrame(columnar)
    keep = [c for c in SUBMISSIONS_COLUMNS if c in df.columns]
    return df[keep] if keep else pd.DataFrame()


@connector(output=SUBMISSIONS_OUTPUT, tags=["sec_edgar", "tool"])
def sec_edgar_submissions(
    cik: str,
    limit: int = 20,
    form: str | None = None,
    include_older: bool = False,
) -> pd.DataFrame:
    """List filings for a CIK from the EDGAR submissions API, newest first.

    Returns accession number + filing date + form type + primary document +
    report date, capped at `limit` rows (1-1000). `form` filters to one exact
    form type (case-insensitive, e.g. "10-K"). `include_older=True` also walks
    the additional `filings.files[]` pages, so filings older than the most-recent
    ~1000 window become reachable (otherwise only the recent window is searched).
    """
    cik_norm = normalize_cik(cik)
    limit_clamped = max(1, min(limit, 1000))

    http = data_client()
    payload = fetch_json(
        http, path=f"/submissions/CIK{cik_norm}.json", provider=PROVIDER, op_name="submissions"
    )
    if not isinstance(payload, dict):
        raise ParseError(PROVIDER, "submissions response was not a JSON object")

    filings = payload.get("filings", {}) if isinstance(payload.get("filings"), dict) else {}
    frames = [_recent_frame(filings.get("recent", {}))]

    if include_older:
        for extra in filings.get("files", []) or []:
            name = extra.get("name") if isinstance(extra, dict) else None
            if not name:
                continue
            # A failed page is fatal (not best-effort): silently dropping a page
            # would under-report a filer's history. fetch_json raises a typed error.
            page = fetch_json(
                http, path=f"/submissions/{name}", provider=PROVIDER, op_name="submissions_page"
            )
            if isinstance(page, dict):
                frames.append(_recent_frame(page))

    df = pd.concat([f for f in frames if not f.empty], ignore_index=True) if any(
        not f.empty for f in frames
    ) else pd.DataFrame()

    if df.empty:
        raise EmptyDataError(
            PROVIDER, message=f"No submissions returned for CIK {cik_norm}", query_params={"cik": cik_norm}
        )

    if form:
        df = df[df["form"].astype(str).str.upper() == form.strip().upper()]
        if df.empty:
            raise EmptyDataError(
                PROVIDER,
                message=f"No {form!r} filings for CIK {cik_norm}",
                query_params={"cik": cik_norm, "form": form},
            )

    df = df.sort_values("filingDate", ascending=False, kind="stable").head(limit_clamped)
    return df.reset_index(drop=True)


# Files in an accession folder that are never the primary document: XBRL
# rendered fragments (R1.htm…), the linkbase/schema technicals, the index pages,
# and the spreadsheet export.
_SKIP_DOC = re.compile(r"(?:^r\d+\.htm)|(?:_(?:def|cal|lab|pre|htm)\.xml$)|(?:\.xsd$)|index", re.IGNORECASE)


def _pick_primary_document(items: list[dict[str, Any]]) -> str | None:
    """Choose the primary document from an accession folder's ``index.json`` items.

    Prefers the largest HTML body (the 10-K/10-Q/8-K document), then an XML
    document (Form 4/13F), then the full-submission ``.txt`` — skipping index
    pages and XBRL technical files. Returns ``None`` if nothing qualifies.
    """
    names = [str(it.get("name", "")) for it in items if isinstance(it, dict) and it.get("name")]
    sizes = {str(it.get("name", "")): int(str(it.get("size") or 0) or 0) for it in items if isinstance(it, dict)}
    candidates = [n for n in names if not _SKIP_DOC.search(n)]

    htmls = [n for n in candidates if n.lower().endswith((".htm", ".html"))]
    if htmls:
        return max(htmls, key=lambda n: sizes.get(n, 0))
    xmls = [n for n in candidates if n.lower().endswith(".xml")]
    if xmls:
        return xmls[0]
    txts = [n for n in candidates if n.lower().endswith(".txt")]
    if txts:
        return txts[0]
    return candidates[0] if candidates else None


@connector(tags=["sec_edgar", "tool"])
def sec_edgar_fetch_filing(
    cik: str, accession_number: str, document: str | None = None
) -> dict[str, str]:
    """Fetch one SEC filing document body from the EDGAR archives.

    When `document` is omitted, resolves the primary document for
    `accession_number` from the filing's `index.json` (works for any filing,
    however old), then returns that document's raw text/HTML body. Returns a dict
    of cik + accession_number + document + content. Note: a filing body (e.g. a
    10-K) can be multiple megabytes.
    """
    cik_norm = normalize_cik(cik)
    accession = accession_number.strip().replace("-", "")
    if not accession:
        raise InvalidParameterError(PROVIDER, "accession_number is required")

    cik_int = str(int(cik_norm))
    folder = f"/Archives/edgar/data/{cik_int}/{accession}"
    www = www_client()

    doc_name = document
    if not doc_name:
        index = fetch_json(www, path=f"{folder}/index.json", provider=PROVIDER, op_name="filing_index")
        items = index.get("directory", {}).get("item", []) if isinstance(index, dict) else []
        doc_name = _pick_primary_document(items if isinstance(items, list) else [])
    if not doc_name:
        # Last resort: the full-submission text file (named with the DASHED accession).
        dashed = (
            f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
            if len(accession) == 18
            else accession_number.strip()
        )
        doc_name = f"{dashed}.txt"

    content = get_text(www, f"{folder}/{doc_name}", op_name="fetch_filing")
    return {
        "cik": cik_norm,
        "accession_number": accession_number,
        "document": doc_name,
        "content": content,
    }
