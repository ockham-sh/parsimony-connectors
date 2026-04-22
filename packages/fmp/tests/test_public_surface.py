"""Public-surface contract.

Pins three invariants:

1. ``CONNECTORS`` contains exactly the 19 expected connector names.
2. Every documented Pydantic param class is importable from the top-level
   module. Tests, downstream wrappers, and type-checking consumers all
   depend on this surface.
3. Every tool-tagged connector's first-line docstring is ≥40 chars (kernel
   MCP requirement) and carries its ``[Starter+]`` / ``[Professional+]``
   / ``[All plans]`` plan-gating prefix so agents can gate tool selection
   on plan tier without attempting the call.
"""

from __future__ import annotations

import re

import parsimony_fmp

_EXPECTED_CONNECTOR_NAMES: frozenset[str] = frozenset(
    {
        "fmp_search",
        "fmp_taxonomy",
        "fmp_quotes",
        "fmp_prices",
        "fmp_company_profile",
        "fmp_peers",
        "fmp_income_statements",
        "fmp_balance_sheet_statements",
        "fmp_cash_flow_statements",
        "fmp_corporate_history",
        "fmp_event_calendar",
        "fmp_analyst_estimates",
        "fmp_news",
        "fmp_insider_trades",
        "fmp_institutional_positions",
        "fmp_earnings_transcript",
        "fmp_index_constituents",
        "fmp_market_movers",
        "fmp_screener",
    }
)

_EXPECTED_PARAM_CLASSES: tuple[str, ...] = (
    "FmpAnalystEstimatesParams",
    "FmpCorporateHistoryParams",
    "FmpEarningsTranscriptParams",
    "FmpEventCalendarParams",
    "FmpFinancialStatementParams",
    "FmpHistoricalPricesParams",
    "FmpIndexConstituentsParams",
    "FmpInsiderTradesParams",
    "FmpInstitutionalPositionsParams",
    "FmpMarketMoversParams",
    "FmpNewsParams",
    "FmpScreenerParams",
    "FmpSearchParams",
    "FmpSymbolParams",
    "FmpSymbolsParams",
    "FmpTaxonomyParams",
)

# The plan-tier prefix that must lead every docstring (kernel MCP surface).
_PLAN_TIER_PREFIX_RE = re.compile(r"^\[(All plans|Starter\+|Professional\+)\]")


def test_connectors_count() -> None:
    assert len(parsimony_fmp.CONNECTORS) == 19


def test_connectors_exactly_expected_names() -> None:
    actual = {c.name for c in parsimony_fmp.CONNECTORS}
    assert actual == _EXPECTED_CONNECTOR_NAMES


def test_every_param_class_importable_from_top_level() -> None:
    for name in _EXPECTED_PARAM_CLASSES:
        assert hasattr(parsimony_fmp, name), f"{name} not exported from parsimony_fmp"


def test_tool_tagged_first_line_long_enough() -> None:
    """Tool-tagged connector docstrings become MCP tool descriptions.

    Kernel contract requires first-line ≥40 chars.
    """
    for c in parsimony_fmp.CONNECTORS:
        if "tool" in c.tags:
            first_line = (c.description or "").splitlines()[0]
            assert len(first_line) >= 40, f"{c.name}: first line too short: {first_line!r}"


def test_every_connector_has_plan_tier_prefix() -> None:
    """Every connector's description begins with a plan-tier prefix.

    An agent that sees ``[Professional+]`` in the tool description knows
    calling the tool on a Starter plan will 402-fail and can route around
    it without attempting the call.
    """
    for c in parsimony_fmp.CONNECTORS:
        first_line = (c.description or "").splitlines()[0]
        match = _PLAN_TIER_PREFIX_RE.match(first_line)
        assert match is not None, f"{c.name}: missing plan-tier prefix in: {first_line!r}"
