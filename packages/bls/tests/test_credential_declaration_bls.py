"""Credential-declaration conformance for parsimony-bls.

BLS is an optional-key provider: the ``registrationkey`` only lifts quotas, so no
verb declares ``requires=`` and none fast-fails on a missing key. The applicable
suite checks are therefore "undeclared does not fast-fail" (the bare call reaches
the network) and "bound secret-param canary reaches the request"; the suite
self-skips the two ``requires=``-dependent checks.

``enumerate_bls_series`` is intentionally omitted: it reads the ``.series`` flat
files over ``curl_cffi`` (Akamai-walled host), which respx cannot intercept, and
it takes no key. The catalog-backed search verbs make no HTTP calls at all.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_bls.connectors.enumerate_surveys import enumerate_bls_surveys
from parsimony_bls.connectors.fetch import bls_fetch


class TestBlsFetchCredentials(CredentialDeclarationSuite):
    connector = bls_fetch
    call_kwargs = {"series_id": "LNS14000000", "start_year": "2026", "end_year": "2026"}
    route_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    method = "POST"


class TestEnumerateBlsSurveysCredentials(CredentialDeclarationSuite):
    connector = enumerate_bls_surveys
    call_kwargs: dict[str, object] = {}
    route_url = "https://api.bls.gov/publicAPI/v2/surveys"
    method = "GET"
