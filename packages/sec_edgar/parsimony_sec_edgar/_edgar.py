"""edgartools bridge: synchronous helpers for normalized financials, Form 4, and 13F-HR.

edgartools is synchronous-only, so every public function here is a plain
synchronous callable invoked directly by the connectors.

``edgar.set_identity(ua)`` is a process-global singleton. We call it at the
top of each sync function so the correct User-Agent is always in effect for
that thread's requests regardless of call order.
"""

from __future__ import annotations

import re

import edgar
import pandas as pd
from parsimony.errors import EmptyDataError, ParseError

PROVIDER = "sec_edgar"

# edgartools' Statement.to_dataframe() names each period column by its period-end
# date, optionally annotated with the fiscal period — e.g. "2025-09-27 (FY)".
# Detect period columns by that key shape rather than blacklisting the (many,
# version-dependent) metadata columns (standard_concept, balance, weight,
# parent_concept, dimension_*, …) — a blacklist silently leaks any unforeseen
# metadata column into the period axis.
_PERIOD_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")

# Form 4 per-transaction column rename: edgartools Title Case → parsimony snake_case.
_FORM4_RENAMES: dict[str, str] = {
    "Transaction Type": "transaction_type",
    "Code": "code",
    "Description": "description",
    "Shares": "shares",
    "Price": "price",
    "Value": "value",
    "Date": "date",
    "Form": "form_type",
    "Issuer": "issuer",
    "Ticker": "ticker",
    "Insider": "insider",
    "Position": "position",
    "Remaining Shares": "remaining_shares",
}

# 13F-HR holdings column rename: edgartools Title Case → parsimony snake_case.
_HOLDINGS_RENAMES: dict[str, str] = {
    "Issuer": "issuer",
    "Class": "security_class",
    "Cusip": "cusip",
    "Ticker": "ticker",
    "Type": "security_type",
    "PutCall": "put_call",
    "SharesPrnAmount": "shares",
    "Value": "value",
    "SoleVoting": "sole_voting",
    "SharedVoting": "shared_voting",
    "NonVoting": "non_voting",
}


def _cik_to_int(cik_padded: str) -> int:
    return int(cik_padded.lstrip("0") or "0")


def _melt_statement(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot a wide Statement DataFrame into a tidy long (concept × period × value) table.

    Only consolidated line items are kept: edgartools emits dimensional breakdown
    rows (per product, per segment) under the same concept, which we drop so each
    (concept, period) is the top-line reported figure. The period is normalized to
    the bare period-end date, dropping the fiscal-period annotation ("(FY)").
    """
    period_cols = [c for c in df.columns if _PERIOD_COL_RE.match(str(c))]
    if not period_cols:
        raise ParseError(PROVIDER, "statement dataframe has no recognizable period columns")
    rows = df
    if "dimension" in rows.columns:
        rows = rows[~rows["dimension"].fillna(False).astype(bool)]
    id_cols = [c for c in ("concept", "label") if c in rows.columns]
    melted = (
        rows.melt(id_vars=id_cols, value_vars=period_cols, var_name="period", value_name="value")
        .dropna(subset=["value"])
        .reset_index(drop=True)
    )
    melted["period"] = melted["period"].astype(str).str.extract(r"^(\d{4}-\d{2}-\d{2})", expand=False)
    return melted


def _sync_get_financials(cik_padded: str, statement: str, ua: str) -> pd.DataFrame:
    """Return a tidy long DataFrame for one annual financial statement.

    ``statement`` is one of: ``"income_statement"``, ``"balance_sheet"``,
    ``"cashflow_statement"`` — matching edgartools ``Financials`` method names.
    """
    edgar.set_identity(ua)
    company = edgar.Company(_cik_to_int(cik_padded))
    financials = company.get_financials()
    if financials is None:
        raise EmptyDataError(
            PROVIDER,
            message=f"No annual financial statements for CIK {cik_padded}",
            query_params={"cik": cik_padded},
        )
    stmt = getattr(financials, statement)()
    if stmt is None:
        label = statement.replace("_", " ")
        raise EmptyDataError(
            PROVIDER,
            message=f"No {label} for CIK {cik_padded}",
            query_params={"cik": cik_padded, "statement": statement},
        )
    try:
        raw = stmt.to_dataframe()
    except Exception as exc:
        raise ParseError(
            PROVIDER,
            f"Failed to parse {statement} for CIK {cik_padded}: {exc}",
        ) from exc
    if raw is None or raw.empty:
        label = statement.replace("_", " ")
        raise EmptyDataError(
            PROVIDER,
            message=f"Empty {label} for CIK {cik_padded}",
            query_params={"cik": cik_padded},
        )
    return _melt_statement(raw)


def _sync_get_insider_transactions(cik_padded: str, limit: int, ua: str) -> pd.DataFrame:
    """Return a per-transaction DataFrame for the most recent ``limit`` Form 4 filings."""
    edgar.set_identity(ua)
    company = edgar.Company(_cik_to_int(cik_padded))
    filings = company.get_filings(form="4")
    frames: list[pd.DataFrame] = []
    for filing in list(filings)[:limit]:
        try:
            form4 = filing.obj()
            df = form4.to_dataframe()
            if df is not None and not df.empty:
                frames.append(df)
        except Exception:
            continue
    if not frames:
        raise EmptyDataError(
            PROVIDER,
            message=f"No Form 4 insider transactions for CIK {cik_padded}",
            query_params={"cik": cik_padded},
        )
    # Drop all-NA columns per frame before concat: Form 4 filings vary in which
    # optional columns they populate, and concatenating frames that carry all-NA
    # columns is deprecated in pandas (and muddies the result dtypes).
    combined = pd.concat([f.dropna(axis=1, how="all") for f in frames], ignore_index=True)
    return combined.rename(columns=_FORM4_RENAMES)


def _sync_get_holdings_13f(cik_padded: str, ua: str) -> pd.DataFrame:
    """Return the most recent 13F-HR aggregated holdings for an institutional filer."""
    edgar.set_identity(ua)
    company = edgar.Company(_cik_to_int(cik_padded))
    filings = company.get_filings(form="13F-HR")
    latest = list(filings)[:1]
    if not latest:
        raise EmptyDataError(
            PROVIDER,
            message=f"No 13F-HR filings for CIK {cik_padded}",
            query_params={"cik": cik_padded},
        )
    try:
        thirteenf = latest[0].obj()
        holdings = thirteenf.holdings
    except Exception as exc:
        raise ParseError(
            PROVIDER,
            f"Failed to parse 13F-HR for CIK {cik_padded}: {exc}",
        ) from exc
    if holdings is None or holdings.empty:
        raise EmptyDataError(
            PROVIDER,
            message=f"Empty 13F-HR holdings for CIK {cik_padded}",
            query_params={"cik": cik_padded},
        )
    return holdings.rename(columns=_HOLDINGS_RENAMES)
