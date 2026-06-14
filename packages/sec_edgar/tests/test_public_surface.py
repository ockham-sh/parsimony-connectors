"""Public-surface contract for parsimony-sec-edgar.

A thin facade: the supported surface is :data:`CONNECTORS`. The twelve verb
callables are re-exported for convenience, but the transport internals
(``_http`` helpers) are not. This test pins that so accidental additions or
removals are caught.
"""

from __future__ import annotations

import parsimony_sec_edgar

EXPECTED_NAMES = {
    "sec_edgar_full_text_search",
    "sec_edgar_find_company",
    "sec_edgar_submissions",
    "sec_edgar_fetch_filing",
    "sec_edgar_company_concept",
    "sec_edgar_company_facts",
    "sec_edgar_frames",
    "sec_edgar_income_statement",
    "sec_edgar_balance_sheet",
    "sec_edgar_cash_flow",
    "sec_edgar_insider_transactions",
    "sec_edgar_holdings_13f",
}


def test_all_is_minimal() -> None:
    assert parsimony_sec_edgar.__all__ == ["CONNECTORS"]


def test_connectors_count() -> None:
    assert len(parsimony_sec_edgar.CONNECTORS) == 12


def test_connector_names() -> None:
    assert {c.name for c in parsimony_sec_edgar.CONNECTORS} == EXPECTED_NAMES


def test_verbs_importable_from_facade() -> None:
    for name in EXPECTED_NAMES:
        assert hasattr(parsimony_sec_edgar, name), name


def test_transport_internals_not_reexported() -> None:
    for name in ("user_agent", "data_client", "www_client", "efts_client", "get_text", "normalize_cik", "fetch_json"):
        assert not hasattr(parsimony_sec_edgar, name), name
