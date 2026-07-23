"""Credential-declaration conformance for parsimony-boj (Bank of Japan).

BoJ is keyless: every connector declares ``requires=()`` and no ``secrets=``.
``test_undeclared_does_not_fast_fail`` proves each HTTP verb reaches the network
with nothing configured; the declared/secret checks self-skip.

Only the HTTP verbs are wired here. ``boj_databases_search`` / ``boj_series_search``
are catalog-backed (they query a local catalog snapshot via ``CatalogLRU``), issue
no provider HTTP at query time, and so have nothing for this suite to assert.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_boj import boj_fetch
from parsimony_boj.connectors.enumerate import enumerate_boj


class TestBojFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = boj_fetch
    call_kwargs = {"db": "FM08", "code": "FXERD01"}
    route_url = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"


class TestEnumerateBojCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_boj
    call_kwargs: dict = {}
    # The crawl fans getMetadata across the DB registry; every request hits this
    # path (query-only variation), so the mocked route is hit on the first DB.
    route_url = "https://www.stat-search.boj.or.jp/api/v1/getMetadata"
