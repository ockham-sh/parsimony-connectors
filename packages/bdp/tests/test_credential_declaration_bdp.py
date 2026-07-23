"""Credential-declaration conformance for parsimony-bdp.

BdP is keyless: every connector declares ``requires=()`` and no ``secrets=``.
``test_undeclared_does_not_fast_fail`` proves each HTTP verb reaches the network
with nothing configured; the declared/secret checks self-skip.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_bdp.connectors.enumerate import enumerate_bdp
from parsimony_bdp.connectors.fetch import bdp_fetch


class TestBdpFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = bdp_fetch
    call_kwargs = {"domain_id": 11, "dataset_id": "ABC"}
    route_url = "https://bpstat.bportugal.pt/data/v1/domains/11/datasets/ABC/"


class TestEnumerateBdpCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_bdp
    call_kwargs: dict = {}
    # First request of the crawl: _list_domains → GET {BASE_URL}/domains/.
    route_url = "https://bpstat.bportugal.pt/data/v1/domains/"
