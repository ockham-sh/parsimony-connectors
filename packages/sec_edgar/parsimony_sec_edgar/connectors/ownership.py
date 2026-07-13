"""SEC EDGAR ownership connectors (via edgartools).

Two verbs:
- ``sec_edgar_insider_transactions`` — Form 4 per-transaction data.
- ``sec_edgar_holdings_13f`` — latest 13F-HR aggregated portfolio holdings.

Both rely on edgartools (synchronous), called directly.
"""

from __future__ import annotations

import pandas as pd
from parsimony.connector import connector

import parsimony_sec_edgar._edgar as _edgar
from parsimony_sec_edgar._http import normalize_cik, user_agent
from parsimony_sec_edgar.outputs import (
    HOLDINGS_13F_COLUMNS,
    HOLDINGS_13F_OUTPUT,
    INSIDER_TRANSACTIONS_COLUMNS,
    INSIDER_TRANSACTIONS_OUTPUT,
)


@connector(output=INSIDER_TRANSACTIONS_OUTPUT, tags=["sec_edgar"])
def sec_edgar_insider_transactions(cik: str, limit: int = 20) -> pd.DataFrame:
    """Recent Form 4 insider transactions for a company.

    Returns one row per disclosed transaction: date, insider name and position,
    issuer, shares, price, value, and transaction type code. ``limit`` caps the
    number of Form 4 filings inspected (default 20); higher values retrieve a
    longer history but make more network requests.

    ``cik`` is the SEC CIK (numeric string); use ``sec_edgar_find_company`` to
    resolve a ticker.
    """
    ua = user_agent()
    cik_padded = normalize_cik(cik)
    df = _edgar._sync_get_insider_transactions(cik_padded, limit, ua)
    df = df.reindex(columns=list(INSIDER_TRANSACTIONS_COLUMNS))
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce")
    return df


@connector(output=HOLDINGS_13F_OUTPUT, tags=["sec_edgar"])
def sec_edgar_holdings_13f(cik: str) -> pd.DataFrame:
    """Latest 13F-HR portfolio holdings for an institutional investment manager.

    Returns the most recent 13F-HR filing's aggregated holdings: issuer name,
    CUSIP, ticker, security class and type, shares held, and reported market
    value. ``put_call`` is populated only for option positions (None otherwise).

    Only institutional investment managers that manage at least $100M in
    qualifying assets are required to file 13F-HR with the SEC.

    ``cik`` is the SEC CIK (numeric string); use ``sec_edgar_find_company`` to
    resolve a ticker or firm name.
    """
    ua = user_agent()
    cik_padded = normalize_cik(cik)
    df = _edgar._sync_get_holdings_13f(cik_padded, ua)
    return df.reindex(columns=list(HOLDINGS_13F_COLUMNS))
