"""Destatis connector parameter models."""

from __future__ import annotations

from typing import Annotated

from parsimony.errors import InvalidParameterError
from pydantic import BaseModel, ConfigDict, Field, field_validator


class DestatisFetchParams(BaseModel):
    """Parameters for fetching Destatis table data.

    The new ``/genesisGONLINE/api/rest/tables/{code}/data`` endpoint takes
    the table code in the URL path. We accept it via the canonical ``name``
    field (matches the legacy GENESIS query param key); ``table_id`` remains
    available as an alias for backwards compatibility with code that still
    constructs ``DestatisFetchParams(table_id=...)``.
    """

    model_config = ConfigDict(populate_by_name=True)

    name: Annotated[str, "ns:destatis"] = Field(
        ...,
        alias="table_id",
        description="GENESIS table code (e.g. 61111-0001).",
    )
    start_year: str | None = Field(default=None, description="Start year (YYYY) — best-effort filter.")
    end_year: str | None = Field(default=None, description="End year (YYYY) — best-effort filter.")

    @field_validator("name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise InvalidParameterError("destatis", "name (table code) must be non-empty")
        return v


class DestatisEnumerateParams(BaseModel):
    """No parameters needed — enumerates all GENESIS-Online statistics."""

    pass
