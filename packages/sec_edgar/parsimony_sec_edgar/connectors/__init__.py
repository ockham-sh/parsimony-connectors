"""sec_edgar connector registry — twelve verbs, no catalog (native search)."""

from __future__ import annotations

from parsimony.connector import Connectors

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

CONNECTORS = Connectors(
    [
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
    ]
)

__all__ = ["CONNECTORS"]
