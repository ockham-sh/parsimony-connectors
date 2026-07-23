"""Credential-declaration contract for parsimony-sec-edgar.

Every verb declares ``requires=("SEC_EDGAR_USER_AGENT",)`` and no ``secrets=``
(the User-Agent is a contact identity read from the env, not a bound/redacted
param). The shared auth path is :func:`parsimony_sec_edgar._http.user_agent`,
called at client construction inside every verb: it fast-fails naming the env
var when unset, and otherwise stamps the value into the outgoing ``User-Agent``
header.

The seven verbs whose HTTP goes through the package's own transport clients
(``data_client`` / ``www_client`` / ``efts_client``) are wired to the full
:class:`CredentialDeclarationSuite` below.

The five edgartools-backed verbs (``sec_edgar_income_statement``,
``sec_edgar_balance_sheet``, ``sec_edgar_cash_flow``,
``sec_edgar_insider_transactions``, ``sec_edgar_holdings_13f``) issue their HTTP
from *inside* the third-party ``edgar`` library, not the package's clients, so
the suite's reaches-request check has no single mockable route to assert against.
Their shared declaration contract — the ``user_agent()`` fast-fail — is covered
by the explicit per-module tests at the bottom of this file.
"""

from __future__ import annotations

import pytest
import respx
from parsimony.errors import UnauthorizedError
from parsimony_test_support import CredentialDeclarationSuite

from parsimony_sec_edgar import (
    sec_edgar_company_concept,
    sec_edgar_company_facts,
    sec_edgar_fetch_filing,
    sec_edgar_find_company,
    sec_edgar_frames,
    sec_edgar_full_text_search,
    sec_edgar_income_statement,
    sec_edgar_insider_transactions,
    sec_edgar_submissions,
)

# --- Verbs on the package's own transport clients -----------------------------


class TestSecEdgarSubmissionsCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_submissions
    call_kwargs = {"cik": "320193"}
    route_url = "https://data.sec.gov/submissions/CIK0000320193.json"


class TestSecEdgarFetchFilingCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_fetch_filing
    # ``document`` supplied so the verb makes a single GET for the body, skipping
    # the index.json resolution crawl.
    call_kwargs = {
        "cik": "320193",
        "accession_number": "0000320193-23-000106",
        "document": "aapl.htm",
    }
    route_url = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl.htm"


class TestSecEdgarFullTextSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_full_text_search
    call_kwargs = {"query": "climate risk"}
    route_url = "https://efts.sec.gov/LATEST/search-index"


class TestSecEdgarFindCompanyCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_find_company
    call_kwargs = {"identifier": "AAPL"}
    route_url = "https://www.sec.gov/files/company_tickers.json"


class TestSecEdgarCompanyConceptCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_company_concept
    call_kwargs = {"cik": "320193", "tag": "Assets"}
    route_url = "https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/Assets.json"


class TestSecEdgarCompanyFactsCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_company_facts
    call_kwargs = {"cik": "320193"}
    route_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"


class TestSecEdgarFramesCredentialDeclaration(CredentialDeclarationSuite):
    connector = sec_edgar_frames
    call_kwargs = {"tag": "Assets", "period": "CY2023"}
    route_url = "https://data.sec.gov/api/xbrl/frames/us-gaap/Assets/USD/CY2023.json"


# --- edgartools-backed verbs: shared user_agent() fast-fail contract ----------
#
# One verb per edgartools-backed module. The reaches-request / reaches-header
# checks are inapplicable (HTTP happens inside the ``edgar`` library, off the
# package's clients), but the declared fast-fail — a bare call with the env var
# absent raising ``UnauthorizedError`` naming ``SEC_EDGAR_USER_AGENT`` before any
# network — is the same contract the suite's first check asserts.


@respx.mock
@pytest.mark.parametrize(
    "connector",
    [sec_edgar_income_statement, sec_edgar_insider_transactions],
    ids=["income_statement", "insider_transactions"],
)
def test_edgartools_verb_fast_fails_naming_env_var(
    connector: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SEC_EDGAR_USER_AGENT", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        connector(cik="320193")  # type: ignore[operator]
    assert exc_info.value.env_var == "SEC_EDGAR_USER_AGENT"
