"""Codelist catalog namespace helpers."""

from __future__ import annotations

from parsimony.catalog import code_token, normalize_namespace

from parsimony_sdmx.connectors._agencies import AgencyId


def codelist_namespace(agency: AgencyId | str, codelist_id: str) -> str:
    """Return ``sdmx_codelist_{agency}_{tokenized_codelist_id}``."""
    raw = agency.value if isinstance(agency, AgencyId) else str(agency)
    token = code_token(codelist_id)
    return normalize_namespace(f"sdmx_codelist_{raw.lower()}_{token}")


__all__ = ["codelist_namespace"]
