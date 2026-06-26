"""Banco de España (BdE): fetch + catalog enumeration."""

from __future__ import annotations

from parsimony_bde.connectors import CONNECTORS, load
from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.connectors.fetch import bde_fetch
from parsimony_bde.search import bde_search

__all__ = ["CONNECTORS", "load", "bde_fetch", "enumerate_bde", "bde_search"]
