"""Credential-declaration contract for parsimony-bdf.

Proves the two keyed HTTP verbs' ``requires=("BDF_API_KEY",)`` /
``secrets=("api_key",)`` declarations match runtime: the bare call fast-fails
naming the env var before any network call, an env-supplied key reaches the
outgoing request (as the ``Apikey`` auth header), and a bound ``api_key`` secret
reaches it too. Wired via :class:`CredentialDeclarationSuite`.

``bdf_search`` is catalog-backed (``make_local_search_connector``) and makes no
HTTP call, so it is keyless by construction and out of scope here.
"""

from __future__ import annotations

from typing import Any

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_bdf.connectors.enumerate import enumerate_bdf
from parsimony_bdf.connectors.fetch import bdf_fetch

_BASE = "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets"


class TestBdfFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = bdf_fetch
    call_kwargs = {"key": "EXR.M.USD.EUR.SP00.E"}
    route_url = f"{_BASE}/observations/exports/json"


class TestBdfEnumerateCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_bdf
    call_kwargs: dict[str, Any] = {}
    # The enumerator's first request is the dataflow-stub export; the auth header
    # carries the resolved key on it, which is all the reaches-request check needs.
    route_url = f"{_BASE}/webstat-datasets/exports/json"
