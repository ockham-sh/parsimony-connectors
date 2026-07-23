"""SEC EDGAR connector output schemas.

EDGAR is **not** a timeseries provider — it exposes four atomic units
(registrant, filing, document, XBRL fact), so each verb declares its own schema
rather than sharing one. ``company_facts`` and ``fetch_filing`` return a raw
``dict`` (no ``OutputSpec``): the former is a deep nested XBRL blob meant for
downstream extraction, the latter a single document body.

Columns that a given query may omit (``start`` on instantaneous XBRL facts,
``period_ending`` on forms without a report period) are emitted as explicit
``None`` so the column always exists. They are declared ``datetime`` (not
``timestamp``/``numeric``), which the framework's all-NaN guard does not police,
so an all-empty slice coerces to ``NaT`` instead of raising.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

# Full-text search: one row per matching filing document. KEY=accession,
# TITLE=the registrant display name. ``cik`` + ``accession`` + ``document`` are
# exactly the arguments an agent passes to ``sec_edgar_fetch_filing``.
FULL_TEXT_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="accession", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="display_name", role=ColumnRole.TITLE),
        Column(name="form", role=ColumnRole.DATA),
        Column(name="filing_date", role=ColumnRole.DATA),
        Column(name="cik", role=ColumnRole.DATA),
        Column(name="document", role=ColumnRole.DATA),
        Column(name="period_ending", role=ColumnRole.DATA),
        Column(name="score", role=ColumnRole.DATA),
    ]
)
FULL_TEXT_SEARCH_COLUMNS: tuple[str, ...] = tuple(c.name for c in FULL_TEXT_SEARCH_OUTPUT.columns)

# Company lookup from the published ticker map. KEY=cik, TITLE=title, DATA=ticker.
FIND_COMPANY_OUTPUT = OutputSpec(
    columns=[
        Column(name="cik", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="ticker", role=ColumnRole.DATA),
    ]
)
FIND_COMPANY_COLUMNS: tuple[str, ...] = tuple(c.name for c in FIND_COMPANY_OUTPUT.columns)

# A filer's filings, newest-first. KEY=accessionNumber.
SUBMISSIONS_OUTPUT = OutputSpec(
    columns=[
        Column(name="accessionNumber", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="filingDate", role=ColumnRole.DATA),
        Column(name="form", role=ColumnRole.DATA),
        Column(name="primaryDocument", role=ColumnRole.DATA),
        Column(name="reportDate", role=ColumnRole.DATA),
    ]
)
SUBMISSIONS_COLUMNS: tuple[str, ...] = tuple(c.name for c in SUBMISSIONS_OUTPUT.columns)

# One XBRL concept's full disclosure history for one company — a tidy long
# timeseries. DATA-only: the concept identity (cik/taxonomy/tag) rides in
# provenance params, not in repeated constant columns. ``unit`` disambiguates a
# company that reports the same concept in more than one unit (e.g. USD + CAD).
COMPANY_CONCEPT_OUTPUT = OutputSpec(
    columns=[
        Column(name="end", role=ColumnRole.DATA),
        Column(name="val", role=ColumnRole.DATA),
        Column(name="unit", role=ColumnRole.DATA),
        Column(name="fy", role=ColumnRole.DATA),
        Column(name="fp", role=ColumnRole.DATA),
        Column(name="form", role=ColumnRole.DATA),
        Column(name="filed", role=ColumnRole.DATA),
        Column(name="accn", role=ColumnRole.DATA),
        Column(name="start", role=ColumnRole.DATA),
    ]
)
COMPANY_CONCEPT_COLUMNS: tuple[str, ...] = tuple(c.name for c in COMPANY_CONCEPT_OUTPUT.columns)

# One concept, one calendrical period, across every reporting entity that filed
# it — a cross-sectional snapshot. KEY=cik, TITLE=entityName.
FRAMES_OUTPUT = OutputSpec(
    columns=[
        Column(name="cik", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="entityName", role=ColumnRole.TITLE),
        Column(name="val", role=ColumnRole.DATA),
        Column(name="end", role=ColumnRole.DATA),
        Column(name="loc", role=ColumnRole.DATA),
        Column(name="accn", role=ColumnRole.DATA),
        Column(name="start", role=ColumnRole.DATA),
    ]
)
FRAMES_COLUMNS: tuple[str, ...] = tuple(c.name for c in FRAMES_OUTPUT.columns)

# Annual financial statement (income statement / balance sheet / cash flow) as a
# tidy long table: one row per (concept × period). concept = the XBRL tag;
# label = human-readable line item; period = reporting date string.
FINANCIAL_STATEMENT_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="concept",
            role=ColumnRole.KEY,
            description=(
                "XBRL tag as the filer chose it. Filers use different tags for the same line "
                "item, so filter on a set, not one name — total revenue is us-gaap_Revenues for "
                "some filers and us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax for "
                "others."
            ),
        ),
        Column(name="label", role=ColumnRole.TITLE),
        Column(name="period", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)
FINANCIAL_STATEMENT_COLUMNS: tuple[str, ...] = tuple(c.name for c in FINANCIAL_STATEMENT_OUTPUT.columns)

# Form 4 insider transactions — one row per disclosed transaction.
INSIDER_TRANSACTIONS_OUTPUT = OutputSpec(
    columns=[
        Column(name="date", role=ColumnRole.DATA),
        Column(name="issuer", role=ColumnRole.TITLE),
        Column(name="ticker", role=ColumnRole.DATA),
        Column(name="insider", role=ColumnRole.DATA),
        Column(name="position", role=ColumnRole.DATA),
        Column(name="transaction_type", role=ColumnRole.DATA),
        Column(name="code", role=ColumnRole.DATA),
        Column(name="shares", role=ColumnRole.DATA),
        Column(name="price", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
        Column(name="remaining_shares", role=ColumnRole.DATA),
    ]
)
INSIDER_TRANSACTIONS_COLUMNS: tuple[str, ...] = tuple(c.name for c in INSIDER_TRANSACTIONS_OUTPUT.columns)

# 13F-HR aggregated holdings — one row per reported security position.
# KEY=cusip. put_call is present only for option positions (None otherwise).
HOLDINGS_13F_OUTPUT = OutputSpec(
    columns=[
        Column(name="cusip", role=ColumnRole.KEY, namespace="sec_edgar"),
        Column(name="issuer", role=ColumnRole.TITLE),
        Column(name="ticker", role=ColumnRole.DATA),
        Column(name="security_class", role=ColumnRole.DATA),
        Column(name="security_type", role=ColumnRole.DATA),
        Column(name="put_call", role=ColumnRole.DATA),
        Column(name="shares", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)
HOLDINGS_13F_COLUMNS: tuple[str, ...] = tuple(c.name for c in HOLDINGS_13F_OUTPUT.columns)
