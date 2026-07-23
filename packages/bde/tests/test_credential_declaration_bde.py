"""Credential-declaration conformance for parsimony-bde.

BdE is keyless: every connector declares ``requires=()`` and no ``secrets=``.
The applicable check is ``test_undeclared_does_not_fast_fail`` — it proves each
HTTP verb reaches the network with nothing configured (no ``UnauthorizedError``
fast-fail). The declared/secret checks self-skip on a keyless connector.
"""

from __future__ import annotations

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.connectors.fetch import bde_fetch


class TestBdeFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = bde_fetch
    call_kwargs = {"key": "D_1NBAF472"}
    route_url = "https://app.bde.es/bierest/resources/srdatosapp/listaSeries"


class TestEnumerateBdeCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_bde
    call_kwargs: dict = {}
    # First catalog chapter fetched by the crawl (CATALOG_CHAPTERS[0] == "be").
    route_url = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv/catalogo_be.csv"
