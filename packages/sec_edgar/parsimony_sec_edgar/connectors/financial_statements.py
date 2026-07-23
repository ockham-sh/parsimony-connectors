"""SEC EDGAR financial statement connectors (via edgartools).

Three verbs — income statement, balance sheet, cash flow — each returning a
tidy long table (concept × period × value) built from the filer's XBRL data.
All three share the same output schema: ``FINANCIAL_STATEMENT_OUTPUT``.

The underlying edgartools library is synchronous; called directly.
"""

from __future__ import annotations

import pandas as pd
from parsimony.connector import connector

import parsimony_sec_edgar._edgar as _edgar
from parsimony_sec_edgar._http import normalize_cik, user_agent
from parsimony_sec_edgar.outputs import FINANCIAL_STATEMENT_COLUMNS, FINANCIAL_STATEMENT_OUTPUT


@connector(output=FINANCIAL_STATEMENT_OUTPUT, tags=["sec_edgar"])
def sec_edgar_income_statement(cik: str) -> pd.DataFrame:
    """Annual income statement for a company, normalized to a tidy long table.

    Returns one row per (XBRL concept × reporting period). ``cik`` is the
    SEC CIK (numeric string, zero-padded or bare); use ``sec_edgar_find_company``
    to resolve a ticker. Sourced from the company's XBRL filings via edgartools.

    Filers disagree on the tag for the same line item, so matching one concept
    name across a peer set silently returns nothing for the peers that chose
    another. Total revenue is the common case: ``us-gaap_Revenues`` (NVDA) vs
    ``us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax`` (AMD, INTC,
    AVGO, TXN). Match a set of aliases, or read the ``concept`` column first —
    an empty selection means "not tagged that way", never "not reported".
    """
    ua = user_agent()
    cik_padded = normalize_cik(cik)
    df = _edgar._sync_get_financials(cik_padded, "income_statement", ua)
    return df[list(FINANCIAL_STATEMENT_COLUMNS)]


@connector(output=FINANCIAL_STATEMENT_OUTPUT, tags=["sec_edgar"])
def sec_edgar_balance_sheet(cik: str) -> pd.DataFrame:
    """Annual balance sheet for a company, normalized to a tidy long table.

    Returns one row per (XBRL concept × reporting period). ``cik`` is the
    SEC CIK (numeric string, zero-padded or bare); use ``sec_edgar_find_company``
    to resolve a ticker. Sourced from the company's XBRL filings via edgartools.
    """
    ua = user_agent()
    cik_padded = normalize_cik(cik)
    df = _edgar._sync_get_financials(cik_padded, "balance_sheet", ua)
    return df[list(FINANCIAL_STATEMENT_COLUMNS)]


@connector(output=FINANCIAL_STATEMENT_OUTPUT, tags=["sec_edgar"])
def sec_edgar_cash_flow(cik: str) -> pd.DataFrame:
    """Annual cash flow statement for a company, normalized to a tidy long table.

    Returns one row per (XBRL concept × reporting period). ``cik`` is the
    SEC CIK (numeric string, zero-padded or bare); use ``sec_edgar_find_company``
    to resolve a ticker. Sourced from the company's XBRL filings via edgartools.
    """
    ua = user_agent()
    cik_padded = normalize_cik(cik)
    df = _edgar._sync_get_financials(cik_padded, "cashflow_statement", ua)
    return df[list(FINANCIAL_STATEMENT_COLUMNS)]
