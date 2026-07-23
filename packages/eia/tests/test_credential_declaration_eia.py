"""Credential-declaration contract for parsimony-eia.

Proves the four keyed HTTP verbs' ``requires=("EIA_API_KEY",)`` /
``secrets=("api_key",)`` declarations match runtime: the bare call fast-fails
naming the env var before any network call, an env-supplied key reaches the
outgoing request (as the ``?api_key=`` query param), and a bound ``api_key``
secret reaches it too. Wired via :class:`CredentialDeclarationSuite`.

``eia_search`` is catalog-backed and keyless, so it is out of scope here.
"""

from __future__ import annotations

from typing import Any

from parsimony_test_support import CredentialDeclarationSuite

from parsimony_eia.connectors.enumerate import enumerate_eia
from parsimony_eia.connectors.fetch import eia_facets, eia_fetch, eia_fetch_series

_BASE = "https://api.eia.gov/v2"


class TestEiaFetchCredentialDeclaration(CredentialDeclarationSuite):
    connector = eia_fetch
    call_kwargs = {"route": "petroleum/pri/spt"}
    route_url = f"{_BASE}/petroleum/pri/spt/data"


class TestEiaFetchSeriesCredentialDeclaration(CredentialDeclarationSuite):
    connector = eia_fetch_series
    call_kwargs = {"series_id": "PET.RWTC.D"}
    route_url = f"{_BASE}/seriesid/PET.RWTC.D"


class TestEiaFacetsCredentialDeclaration(CredentialDeclarationSuite):
    connector = eia_facets
    call_kwargs = {"route": "petroleum/pri/spt", "facet": "product"}
    route_url = f"{_BASE}/petroleum/pri/spt/facet/product"


class TestEiaEnumerateCredentialDeclaration(CredentialDeclarationSuite):
    connector = enumerate_eia
    call_kwargs: dict[str, Any] = {}
    # The enumerator walks the route tree starting from the API root; the key
    # rides as the ``?api_key=`` default query param on that first request.
    route_url = f"{_BASE}/"
