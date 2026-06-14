"""Destatis (German Federal Statistical Office): fetch + catalog enumeration."""

from __future__ import annotations

from parsimony_destatis.connectors import CONNECTORS, load

__all__ = ["CONNECTORS", "load"]
