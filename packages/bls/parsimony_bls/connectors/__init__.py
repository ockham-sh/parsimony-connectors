"""The BLS connector collection (two enumerators + fetch + two search tools)."""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bls.connectors.enumerate_series import enumerate_bls_series
from parsimony_bls.connectors.enumerate_surveys import enumerate_bls_surveys
from parsimony_bls.connectors.fetch import bls_fetch
from parsimony_bls.connectors.search import bls_series_search, bls_surveys_search

CONNECTORS = Connectors(
    [
        bls_fetch,
        enumerate_bls_surveys,
        enumerate_bls_series,
        bls_surveys_search,
        bls_series_search,
    ]
)

__all__ = ["CONNECTORS"]
