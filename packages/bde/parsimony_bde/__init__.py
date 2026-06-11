"""Banco de España (BdE): fetch + catalog enumeration."""

from __future__ import annotations

from parsimony_bde.connectors import CONNECTORS, load

__all__ = ["CONNECTORS", "load"]
