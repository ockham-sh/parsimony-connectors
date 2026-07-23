"""Credential-declaration conformance for parsimony-treasury.

US Treasury (Fiscal Data + Office of Debt Management XML feeds) is keyless:
every connector declares ``requires=()`` and no ``secrets=``.
``test_undeclared_does_not_fast_fail`` proves each HTTP verb reaches the network
with nothing configured; the declared/secret checks self-skip.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_treasury import (
    enumerate_treasury,
    treasury_fetch,
    treasury_rates_fetch,
)


class TestTreasuryFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = treasury_fetch
    call_kwargs = {"endpoint": "v2/accounting/od/debt_to_penny"}
    route_url = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
        "v2/accounting/od/debt_to_penny"
    )


class TestTreasuryRatesFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = treasury_rates_fetch
    call_kwargs = {"feed": "daily_treasury_yield_curve"}
    route_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"


class TestEnumerateTreasuryCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_treasury
    call_kwargs: dict = {}
    # First request of the crawl: GET /services/dtg/metadata/ (datasets index).
    route_url = "https://api.fiscaldata.treasury.gov/services/dtg/metadata/"
