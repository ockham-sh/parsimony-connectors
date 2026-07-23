"""Credential-declaration contract for parsimony-fred.

Proves each connector's ``requires=("FRED_API_KEY",)`` / ``secrets=("api_key",)``
declaration matches runtime: the bare call fast-fails naming the env var before
any network call, an env-supplied key reaches the outgoing request, and a bound
``api_key`` secret reaches it too. Wired via :class:`CredentialDeclarationSuite`.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_fred import fred_fetch, fred_search


class TestFredSearchCredentialDeclaration(CredentialDeclarationSuite):
    connector = fred_search
    call_kwargs = {"query": "unemployment"}
    route_url = "https://api.stlouisfed.org/fred/series/search"


class TestFredFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = fred_fetch
    call_kwargs = {"series_id": "UNRATE"}
    route_url = "https://api.stlouisfed.org/fred/series/observations"
