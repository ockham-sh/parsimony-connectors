"""SEC EDGAR connectors for parsimony.

SEC EDGAR is a **keyless** public source, but SEC's fair-access policy (max 10
requests/second) *requires* every request to carry a ``User-Agent`` header that
identifies the requester (a name and contact email, e.g.
``"Acme Research contact@acme.com"``). A generic or missing User-Agent gets a
``403``/``429``. The header is supplied via the mandatory
``SEC_EDGAR_USER_AGENT`` environment variable and resolved before any network
call. It is required *infrastructure*, not a secret credential — a header (never
a query param, so never logged/redacted) — so it is **not** declared via
``secrets=``/``bind()``/``load()``. There is no API key.

EDGAR exposes four atomic units (registrant, filing, document, XBRL fact).
Discovery is native (EDGAR full-text search over filing content), so there is
no built catalog. Financial statements, insider transactions, and 13F-HR
holdings are parsed via ``edgartools`` (MIT). Exports :data:`CONNECTORS`:

* ``sec_edgar_full_text_search`` — native full-text search over all filing
  content (2001→present, every filer).
* ``sec_edgar_find_company`` — fast exact ticker/CIK/name lookup (ticker map).
* ``sec_edgar_submissions`` — a filer's filings, newest-first (optionally the
  full history via ``include_older``).
* ``sec_edgar_fetch_filing`` — one filing document body.
* ``sec_edgar_company_concept`` — one XBRL concept's full history for a company.
* ``sec_edgar_company_facts`` — the raw XBRL company-facts blob.
* ``sec_edgar_frames`` — one XBRL concept, one period, across all companies.
* ``sec_edgar_income_statement`` — annual income statement (tidy long table).
* ``sec_edgar_balance_sheet`` — annual balance sheet (tidy long table).
* ``sec_edgar_cash_flow`` — annual cash flow statement (tidy long table).
* ``sec_edgar_insider_transactions`` — Form 4 per-transaction insider data.
* ``sec_edgar_holdings_13f`` — latest 13F-HR aggregated portfolio holdings.
"""

from __future__ import annotations

from parsimony_sec_edgar.connectors import CONNECTORS
from parsimony_sec_edgar.connectors.filings import sec_edgar_fetch_filing, sec_edgar_submissions
from parsimony_sec_edgar.connectors.financial_statements import (
    sec_edgar_balance_sheet,
    sec_edgar_cash_flow,
    sec_edgar_income_statement,
)
from parsimony_sec_edgar.connectors.ownership import (
    sec_edgar_holdings_13f,
    sec_edgar_insider_transactions,
)
from parsimony_sec_edgar.connectors.search import sec_edgar_find_company, sec_edgar_full_text_search
from parsimony_sec_edgar.connectors.xbrl import (
    sec_edgar_company_concept,
    sec_edgar_company_facts,
    sec_edgar_frames,
)

__all__ = ["CONNECTORS"]

# Re-exported for convenience / back-compat; the supported surface is CONNECTORS.
_VERBS = (
    sec_edgar_full_text_search,
    sec_edgar_find_company,
    sec_edgar_submissions,
    sec_edgar_fetch_filing,
    sec_edgar_company_concept,
    sec_edgar_company_facts,
    sec_edgar_frames,
    sec_edgar_income_statement,
    sec_edgar_balance_sheet,
    sec_edgar_cash_flow,
    sec_edgar_insider_transactions,
    sec_edgar_holdings_13f,
)
