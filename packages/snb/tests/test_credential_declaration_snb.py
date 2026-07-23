"""Credential-declaration conformance for parsimony-snb (Swiss National Bank).

SNB is keyless: every connector declares ``requires=()`` and no ``secrets=``.
``test_undeclared_does_not_fast_fail`` proves each HTTP verb reaches the network
with nothing configured; the declared/secret checks self-skip.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_snb import snb_fetch
from parsimony_snb.connectors.enumerate import enumerate_snb


class TestSnbFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = snb_fetch
    call_kwargs = {"cube_id": "rendoblim"}
    route_url = "https://data.snb.ch/api/cube/rendoblim/data/csv/en"


class TestEnumerateSnbCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_snb
    call_kwargs: dict = {}
    # First request of the crawl: GET /sitemap (the authoritative cube universe).
    route_url = "https://data.snb.ch/sitemap"
