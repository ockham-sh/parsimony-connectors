"""Banque de France (BdF): fetch + catalog enumeration."""

from __future__ import annotations

from parsimony_bdf.connectors import CONNECTORS, load

__all__ = ["CONNECTORS", "load"]
