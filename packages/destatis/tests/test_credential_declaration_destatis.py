"""Credential-declaration conformance for parsimony-destatis.

Destatis (GENESIS-Online) is keyless here: every connector declares
``requires=()`` and no ``secrets=``. ``test_undeclared_does_not_fast_fail``
proves each HTTP verb reaches the network with nothing configured; the
declared/secret checks self-skip.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_destatis.connectors.enumerate import enumerate_destatis
from parsimony_destatis.connectors.fetch import destatis_fetch


class TestDestatisFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = destatis_fetch
    call_kwargs = {"name": "61111-0001"}
    route_url = "https://genesis.destatis.de/genesis/api/rest/tables/61111-0001/data"


class TestEnumerateDestatisCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_destatis
    call_kwargs: dict = {}
    # First request of the crawl: GET /statistics (the statistics index).
    route_url = "https://genesis.destatis.de/genesis/api/rest/statistics"
