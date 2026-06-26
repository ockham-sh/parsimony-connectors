"""US Bureau of Labor Statistics connector for parsimony.

BLS serves the entire US official labor-statistics universe — CPI, PPI,
employment (CES/CPS/SM/QCEW), unemployment (LAUS), JOLTS, ECI, productivity,
import/export prices, and more — as numeric time series. The universe is far too
large to embed whole (~tens of millions of series), so discovery is **two-tier**,
mirroring ``parsimony-sdmx``: a small always-built *surveys* catalog plus
per-survey *series* catalogs built for the headline surveys. Every series remains
fetchable by id via ``bls_fetch`` regardless of catalog coverage.

Auth: optional ``registrationkey`` (env ``BLS_API_KEY``) — raises quota only. The
bulk flat-file host is Akamai-walled and reached via curl_cffi impersonation.
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bls.connectors import CONNECTORS

__all__ = ["CONNECTORS", "load"]


def load(*, api_key: str = "") -> Connectors:
    """Return :data:`CONNECTORS` with ``api_key`` bound on connectors that accept it."""
    return CONNECTORS.bind(api_key=api_key)
