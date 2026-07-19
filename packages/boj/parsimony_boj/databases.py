"""The Bank of Japan statistics-database registry (archetype C).

The BOJ Time-Series Data Search API exposes **no method to enumerate
databases** — ``getMetadata`` *requires* a ``db`` parameter, and there is no
``getDbList``/``getStatsList`` endpoint. So the 50-DB list is frozen here, with
the archetype-C guards the guidebook mandates:

* **Cross-validated.** These 50 codes/categories/titles are transcribed from the
  official API manual §II.3.(2) **and** match the machine-readable
  ``api_tool.xlsx`` ``DB_Name`` sheet exactly (zero diff, 2026-06-09).
* **Reproducible.** ``scripts/harvest_databases.py`` regenerates this tuple from
  the live XLSX, so the freeze is re-derivable rather than hand-maintained.
* **Floor + shape tested.** A test pins ``len == 50`` and asserts the historical
  phantom ``BP02`` is absent (the list once drifted: 45 + phantom → canonical 50).

Re-run the harvester whenever BoJ revises the manual.
"""

from __future__ import annotations

from parsimony.errors import InvalidParameterError

#: ``(code, category, title)`` — canonical, harvested from ``api_tool.xlsx``
#: ``DB_Name`` sheet (2026-06-09) and verified against the API manual §II.3.(2).
_BOJ_DATABASES: tuple[tuple[str, str, str], ...] = (
    (
        "IR01",
        "Interest Rates on Deposits and Loans",
        'The Basic Discount Rates and Basic Loan Rates (Previously Indicated as "Official Discount Rates")',
    ),
    (
        "IR02",
        "Interest Rates on Deposits and Loans",
        "Average Interest Rates Posted at Financial Institutions by Type of Deposit",
    ),
    ("IR03", "Interest Rates on Deposits and Loans", "Average Interest Rates on Time Deposits by Term"),
    ("IR04", "Interest Rates on Deposits and Loans", "Average Contract Interest Rates on Loans and Discounts"),
    ("FM01", "Financial Markets", "Uncollateralized Overnight Call Rate (average) (Updated every business day)"),
    ("FM02", "Financial Markets", "Short-term Money Market Rates"),
    ("FM03", "Financial Markets", "Amounts Outstanding in Short-term Money Market"),
    ("FM04", "Financial Markets", "Amounts Outstanding in the Call Money Market"),
    ("FM05", "Financial Markets", "Issuance, Redemption, and Outstanding of Public and Corporate Bonds"),
    (
        "FM06",
        "Financial Markets",
        "Trading of Interest-bearing Government Bonds by Purchaser (Interest-bearing Government Bonds)",
    ),
    (
        "FM07",
        "Financial Markets",
        "(Reference)Government Bonds Sales Over the Counter / Counter Sales Ratio (through January 2004)",
    ),
    ("FM08", "Financial Markets", "Foreign Exchange Rates"),
    ("FM09", "Financial Markets", "Effective Exchange Rate"),
    ("PS01", "Payment and Settlement", "Other Payment and Settlement Systems"),
    ("PS02", "Payment and Settlement", "Basic Figures on Fails"),
    ("MD01", "Money, Deposits and Loans", "Monetary Base"),
    ("MD02", "Money, Deposits and Loans", "Money Stock"),
    ("MD03", "Money, Deposits and Loans", "Monetary Survey"),
    ("MD04", "Money, Deposits and Loans", "(Reference) Changes in Money Stock (M2+CDs) and Credit"),
    ("MD05", "Money, Deposits and Loans", "Currency in Circulation"),
    (
        "MD06",
        "Money, Deposits and Loans",
        "Sources of Changes in Current Account Balances at the Bank of Japan and Market Operations (Final Figures)",
    ),
    ("MD07", "Money, Deposits and Loans", "Reserves"),
    ("MD08", "Money, Deposits and Loans", "BOJ Current Account Balances by Sector"),
    ("MD09", "Money, Deposits and Loans", "Monetary Base and the Bank of Japan's Transactions"),
    ("MD10", "Money, Deposits and Loans", "Amounts Outstanding of Deposits by Depositor"),
    ("MD11", "Money, Deposits and Loans", "Deposits, Vault Cash, and Loans and Bills Discounted"),
    (
        "MD12",
        "Money, Deposits and Loans",
        "Deposits, Vault Cash, and Loans and Bills Discounted by Prefecture (Domestically Licensed Banks)",
    ),
    ("MD13", "Money, Deposits and Loans", "Principal Figures of Financial Institutions"),
    ("MD14", "Money, Deposits and Loans", "Time Deposits: Amounts Outstanding and New Deposits by Maturity"),
    ("LA01", "Money, Deposits and Loans", "Loans and Bills Discounted by Sector"),
    ("LA02", "Money, Deposits and Loans", "Loans and Discounts by the Bank of Japan"),
    ("LA03", "Money, Deposits and Loans", "Outstanding of Loans (Others)"),
    ("LA04", "Money, Deposits and Loans", "Commitment Lines Extended by Japanese Banks"),
    (
        "LA05",
        "Money, Deposits and Loans",
        "Senior Loan Officer Opinion Survey on Bank Lending Practices at Large Japanese Banks",
    ),
    ("BS01", "Balance Sheets of the Bank of Japan and Financial Institutions", "Bank of Japan Accounts"),
    ("BS02", "Balance Sheets of the Bank of Japan and Financial Institutions", "Financial Institutions Accounts"),
    ("FF", "Flow of Funds", "Flow of Funds"),
    ("OB01", "Other Bank of Japan Statistics", "Bank of Japan's Transactions with the Government"),
    ("OB02", "Other Bank of Japan Statistics", "Collateral Accepted by the Bank of Japan"),
    ("CO", "TANKAN", "TANKAN"),
    ("PR01", "Prices", "Corporate Goods Price Index (CGPI)"),
    ("PR02", "Prices", "Services Producer Price Index (SPPI)"),
    ("PR03", "Prices", "Input-Output Price Index of the Manufacturing Industry by Sector (IOPI)"),
    ("PR04", "Prices", "<Satellite series> Final Demand-Intermediate Demand price indexes (FD-ID price indexes)"),
    ("PF01", "Public Finance", "Statement of Receipts and Payments of the Treasury Accounts"),
    ("PF02", "Public Finance", "National Government Debt"),
    ("BP01", "Balance of Payments and BIS-Related Statistics", "Balance of Payments"),
    (
        "BIS",
        "Balance of Payments and BIS-Related Statistics",
        "BIS International Locational Banking Statistics and BIS International Consolidated Banking Statistics in Japan",
    ),
    ("DER", "Balance of Payments and BIS-Related Statistics", "Regular Derivatives Market Statistics in Japan"),
    ("OT", "Others", "Others"),
)


def _resolve_boj_database(db_code: str) -> tuple[str, str, str]:
    """Return the ``(code, category, title)`` triple for a DB code (case-insensitive).

    Raises :class:`InvalidParameterError` (not a bare ``ValueError``) for an
    unknown code, so a bad ``db`` argument surfaces as a typed connector error.
    """
    normalized = db_code.strip().upper()
    for code, category, title in _BOJ_DATABASES:
        if code == normalized:
            return code, category, title
    raise InvalidParameterError("boj", f"Unknown BoJ database {db_code!r}")


__all__ = ["_BOJ_DATABASES", "_resolve_boj_database"]
