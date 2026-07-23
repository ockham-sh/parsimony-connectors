"""Credential-declaration conformance for parsimony-boc (Bank of Canada).

BoC is keyless: every connector declares ``requires=()`` and no ``secrets=``.
``test_undeclared_does_not_fast_fail`` proves each HTTP verb reaches the network
with nothing configured; the declared/secret checks self-skip.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_boc import boc_fetch
from parsimony_boc.connectors.enumerate import enumerate_boc


class TestBocFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = boc_fetch
    call_kwargs = {"series_name": "FXUSDCAD"}
    route_url = "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json"


class TestEnumerateBocCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_boc
    call_kwargs: dict = {}
    # First request of the crawl: GET /lists/series/json (the series universe).
    route_url = "https://www.bankofcanada.ca/valet/lists/series/json"
