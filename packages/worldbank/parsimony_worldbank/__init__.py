"""World Bank connector for parsimony.

The World Bank API v2 is a **keyless** public JSON API — no registration, no
api_key, no secrets=/bind()/load(). It serves development indicators (GDP,
population, trade, etc.) per country and year.

The connector provides two functions:
- ``worldbank_fetch`` — fetch one indicator for one country (or all countries)
  as a DataFrame with one row per (country, year).
- ``worldbank_search`` — search the indicator catalogue by keyword.
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_worldbank.connectors import CONNECTORS
from parsimony_worldbank.connectors.search import worldbank_search


def load() -> Connectors:
    """Return :data:`CONNECTORS`.

    World Bank has no auth, so ``load()`` takes no parameters — unlike
    key-gated connectors that accept ``api_key=``.
    """
    return CONNECTORS


__all__ = ["CONNECTORS", "load", "worldbank_search"]
