"""Banco de Portugal (BdP): fetch + catalog enumeration.

Keyless BPstat (Opendatasoft-independent JSON-stat API).
"""

from __future__ import annotations

from parsimony.connector import Connectors

from parsimony_bdp.connectors import CONNECTORS


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog URL bound on search."""
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)


__all__ = ["CONNECTORS", "load"]
