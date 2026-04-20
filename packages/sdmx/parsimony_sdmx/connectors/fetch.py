"""``sdmx_fetch`` — live SDMX retrieval connector.

Thin wrapper over the ported legacy ``_legacy_sdmx.sdmx_fetch`` logic that
adds (a) strict param validation via a closed agency allowlist, (b) a
single outer ``asyncio.timeout`` budget, and (c) bounded retries on
transient transport failures. The heavy lifting — DSD resolution, series
title composition, codelist label maps — stays in ``_legacy_sdmx`` until
Task 15's cleanup pass carves it into ``core/`` and ``io/``.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Annotated

from parsimony.connector import connector
from parsimony.errors import EmptyDataError, ParseError, ProviderError
from parsimony.result import Result
from pydantic import BaseModel, Field, field_validator
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout

from parsimony_sdmx.connectors._agencies import ALL_AGENCIES

logger = logging.getLogger(__name__)

# Single place that names the retry budget. Kept small — SDMX endpoints are
# not uniformly flaky; deep retries amplify pool pressure under load.
_MAX_RETRIES = 2
_RETRY_BASE_DELAY_SEC = 0.5
_RETRY_MAX_DELAY_SEC = 4.0
_FETCH_TIMEOUT_SEC = 45.0

#: Regex-style key validators — reject characters that could escape the SDMX
#: URL path. SDMX uses ``.`` as the dimension separator and ``+`` as OR.
_DATASET_KEY_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._\-]{0,127}$"
_SERIES_KEY_PATTERN = r"^[A-Za-z0-9._+\-]*(?:\.[A-Za-z0-9._+\-]*){0,31}$"


class SdmxFetchParams(BaseModel):
    """Parameters for :func:`sdmx_fetch`.

    ``dataset_key`` is the SDMX ``agency-dataset_id`` form expected by the
    live fetcher (e.g. ``"ECB-YC"``). Agency prefix is independently
    validated against :data:`ALL_AGENCIES`.
    """

    dataset_key: Annotated[
        str,
        Field(
            min_length=3,
            max_length=192,
            description="SDMX dataset identifier prefixed by agency (e.g. 'ECB-YC').",
        ),
    ]
    series_key: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            pattern=_SERIES_KEY_PATTERN,
            description="Dot-separated dimension values identifying the series.",
        ),
    ]
    start_period: str | None = Field(default=None, max_length=32, description="Start period filter (e.g. 2020-01).")
    end_period: str | None = Field(default=None, max_length=32, description="End period filter (e.g. 2024-12).")

    @field_validator("dataset_key")
    @classmethod
    def _validate_dataset_key(cls, v: str) -> str:
        stripped = v.strip()
        if "-" not in stripped:
            raise ValueError("dataset_key must include agency prefix (e.g. 'ECB-YC')")
        agency, dataset_id = stripped.split("-", 1)
        allowed = {a.value for a in ALL_AGENCIES}
        if agency.upper() not in allowed:
            raise ValueError(f"Unknown agency {agency!r}; allowed: {sorted(allowed)}")
        import re

        if not re.match(_DATASET_KEY_PATTERN, dataset_id):
            raise ValueError(f"dataset_id {dataset_id!r} contains disallowed characters")
        return f"{agency.upper()}-{dataset_id}"


def _is_retryable(exc: BaseException) -> bool:
    """Transient errors worth a bounded retry: connect / timeout / 5xx."""
    if isinstance(exc, (Timeout, RequestsConnectionError)):
        return True
    if isinstance(exc, HTTPError):
        status = getattr(getattr(exc, "response", None), "status_code", 0)
        return status in (502, 503, 504) or status == 429
    return False


@connector(tags=["sdmx"])
async def sdmx_fetch(params: SdmxFetchParams) -> Result:
    """Fetch an SDMX time series from the live agency endpoint.

    Dataset_key format: ``{AGENCY}-{DATASET_ID}`` where AGENCY is one of the
    supported SDMX sources (ECB, ESTAT, IMF_DATA, WB_WDI).

    Returns a tabular :class:`Result` with one row per observation, columns
    series_key + title + per-dimension metadata + TIME_PERIOD + value.

    Emits ``ProviderError`` on non-retryable transport failure,
    ``EmptyDataError`` on empty observation set, ``ParseError`` when the
    SDMX payload can't be reduced to the expected schema. Retries bounded
    to :data:`_MAX_RETRIES` on transient 5xx / timeout / connect errors.
    Never forwards raw upstream response bodies — they're a prompt-injection
    surface when this tool is exposed to LLM agents.
    """
    from parsimony_sdmx._legacy_sdmx import SdmxFetchParams as _LegacyParams
    from parsimony_sdmx._legacy_sdmx import sdmx_fetch as _legacy_fetch

    legacy_params = _LegacyParams(
        dataset_key=params.dataset_key,
        series_key=params.series_key,
        start_period=params.start_period,
        end_period=params.end_period,
    )

    attempt = 0
    while True:
        attempt += 1
        try:
            async with asyncio.timeout(_FETCH_TIMEOUT_SEC):
                return await _legacy_fetch(legacy_params)
        except (ProviderError, EmptyDataError, ParseError):
            # Already-taxonomized failure — bubble up unchanged.
            raise
        except TimeoutError as exc:
            raise ProviderError(
                provider="sdmx",
                status_code=0,
                message=f"SDMX fetch exceeded {_FETCH_TIMEOUT_SEC:.0f}s budget for {params.dataset_key}.",
            ) from exc
        except Exception as exc:
            if attempt <= _MAX_RETRIES and _is_retryable(exc):
                delay = min(
                    _RETRY_MAX_DELAY_SEC,
                    _RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.2),
                )
                logger.warning(
                    "sdmx_fetch transient error (attempt %d/%d), retrying in %.2fs: %s",
                    attempt,
                    _MAX_RETRIES + 1,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                continue
            # Non-retryable or retries exhausted — wrap as ProviderError without
            # leaking upstream response body text (prompt-injection surface).
            status = 0
            if isinstance(exc, HTTPError):
                status = getattr(getattr(exc, "response", None), "status_code", 0) or 0
            raise ProviderError(
                provider="sdmx",
                status_code=status,
                message=(
                    f"SDMX fetch failed for {params.dataset_key}/{params.series_key}: "
                    f"{type(exc).__name__}."
                ),
            ) from exc
