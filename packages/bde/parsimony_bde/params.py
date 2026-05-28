"""BdE connector parameter models."""

from __future__ import annotations

from typing import Annotated

from parsimony.errors import InvalidParameterError
from pydantic import BaseModel, Field, field_validator


class BdeFetchParams(BaseModel):
    """Parameters for fetching Banco de España time series."""

    key: Annotated[str, "ns:bde"] = Field(
        ...,
        description="Comma-separated BdE series codes (e.g. D_1NBAF472)",
    )
    time_range: str | None = Field(
        default=None,
        description=("Time range: 30M, 60M, MAX, or a year (e.g. 2024). Default uses the full available range."),
    )
    lang: str = Field(default="en", description="Language: en or es")

    @field_validator("key")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise InvalidParameterError("bde", "At least one series code required")
        return v

    @field_validator("time_range")
    @classmethod
    def _valid_range(cls, v: str | None) -> str | None:
        if v is None:
            return v
        v = v.strip()
        _VALID_RANGES = {"30M", "60M", "MAX"}
        if v.upper() in _VALID_RANGES or v.isdigit():
            return v
        raise InvalidParameterError("bde", f"Invalid time_range '{v}'. Use 30M, 60M, MAX, or a year (e.g. 2024).")

    @field_validator("lang")
    @classmethod
    def _valid_lang(cls, v: str) -> str:
        if v not in ("en", "es"):
            raise InvalidParameterError("bde", "lang must be 'en' or 'es'")
        return v


class BdeEnumerateParams(BaseModel):
    """No parameters needed — discovers series from BdE's published catalog CSVs."""

    pass
