"""``sdmx_fetch`` — live SDMX retrieval connector.

Performs strict param validation against a closed agency allowlist,
budgets the SDMX round-trip with a single outer ``asyncio.timeout``,
and applies bounded retries on transient transport failures.

The body imports ``sdmx`` and ``pandas`` lazily so that just importing
``parsimony_sdmx`` (which the parent CLI does to enumerate
``CATALOGS`` / ``CONNECTORS``) does not drag ``sdmx1`` into the parent
process — guarded by ``tests/test_listing.py::test_plugin_surface_import_does_not_pull_sdmx``.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Annotated

from parsimony.catalog import code_token, normalize_code
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, ParseError, ProviderError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from pydantic import BaseModel, Field, field_validator
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout

from parsimony_sdmx.connectors._agencies import ALL_AGENCIES
from parsimony_sdmx.core.titles import compose_observation_title, format_code_with_label
from parsimony_sdmx.providers.dataset_urls import build_sdmx_dataset_url

logger = logging.getLogger(__name__)

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


def _sdmx_fetch_output(namespace: str, dimension_ids: list[str]) -> OutputConfig:
    """Tabular schema for one dataset fetch."""
    cols: list[Column] = [
        Column(
            name="series_key",
            role=ColumnRole.KEY,
            param_key="series_key",
            namespace=namespace,
        ),
        Column(name="title", role=ColumnRole.TITLE),
    ]
    for dim_id in dimension_ids:
        cols.append(Column(name=dim_id, role=ColumnRole.METADATA))
    cols.extend(
        [
            Column(name="TIME_PERIOD", dtype="datetime", role=ColumnRole.DATA),
            Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        ]
    )
    return OutputConfig(columns=cols)


def _sdmx_namespace_from_dataset_key(dataset_key: str) -> str:
    """Catalog namespace for one SDMX dataset (``sdmx_<tokenized_dataset>``)."""
    return normalize_code(f"sdmx_{code_token(dataset_key)}")


def _is_retryable(exc: BaseException) -> bool:
    """Transient errors worth a bounded retry: connect / timeout / 5xx / 429."""
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
    attempt = 0
    while True:
        attempt += 1
        try:
            async with asyncio.timeout(_FETCH_TIMEOUT_SEC):
                return await _do_sdmx_fetch(params)
        except (ProviderError, EmptyDataError, ParseError):
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


async def _do_sdmx_fetch(params: SdmxFetchParams) -> Result:
    """Inner fetch — performs SDMX I/O and shapes the observation table.

    Imports ``sdmx`` / ``pandas`` and the provider helpers function-locally
    to keep the parent ``parsimony_sdmx`` import graph free of ``sdmx1``.
    """
    import pandas as pd
    import sdmx as sdmx_lib

    from parsimony_sdmx.core.codelists import resolve_codelists
    from parsimony_sdmx.core.errors import SdmxFetchError
    from parsimony_sdmx.providers.sdmx_client import sdmx_client
    from parsimony_sdmx.providers.sdmx_extract import (
        extract_dsd_dim_order,
        extract_raw_codelists,
    )
    from parsimony_sdmx.providers.sdmx_flow import (
        fetch_dataflow_with_structure,
        resolve_dsd,
    )

    agency_id, dataset_id = params.dataset_key.split("-", 1)

    with sdmx_client(agency_id, wb_url_rewrite=True) as client:
        try:
            structure_msg = fetch_dataflow_with_structure(client, dataset_id)
            try:
                dataflow = structure_msg.dataflow[dataset_id]
            except (KeyError, AttributeError, TypeError) as exc:
                raise ProviderError(
                    provider="sdmx",
                    status_code=0,
                    message=f"Dataflow {dataset_id!r} missing from structure response.",
                ) from exc
            dsd = resolve_dsd(client, structure_msg, dataflow, dataset_id)
            data_msg = await asyncio.to_thread(
                client.get,
                resource_type="data",
                resource_id=dataset_id,
                key=params.series_key,
                params={
                    "startPeriod": params.start_period,
                    "endPeriod": params.end_period,
                },
            )
        except HTTPError:
            raise
        except SdmxFetchError as exc:
            cause = exc.__cause__
            if isinstance(cause, HTTPError):
                raise cause from exc
            raise ProviderError(
                provider="sdmx",
                status_code=0,
                message=f"Failed to fetch structure for {dataset_id}: {type(exc).__name__}.",
            ) from exc

    raw = sdmx_lib.to_pandas(data_msg.data)
    df = (
        raw.rename("value").to_frame().reset_index()
        if isinstance(raw, pd.Series)
        else pd.DataFrame(raw).reset_index()
    )
    if df.empty:
        raise EmptyDataError(provider="sdmx", message="No data returned for requested series.")

    if "value" not in df.columns:
        value_columns = [col for col in df.columns if col not in {"TIME_PERIOD"}]
        if len(value_columns) != 1:
            raise ParseError(provider="sdmx", message="Unable to determine SDMX value column")
        df = df.rename(columns={value_columns[0]: "value"})

    dsd_dim_ids = extract_dsd_dim_order(dsd, exclude_time=True)
    if not dsd_dim_ids:
        raise ParseError(
            provider="sdmx",
            message="Unable to determine SDMX series dimensions for series_key",
        )
    missing = [dim_id for dim_id in dsd_dim_ids if dim_id not in df.columns]
    if missing:
        raise ParseError(
            provider="sdmx",
            message=(
                "Unable to align SDMX result columns to DSD order; missing dimension "
                f"column(s) {missing}. Available columns: {list(df.columns)}"
            ),
        )

    for dim_id in dsd_dim_ids:
        df[dim_id] = df[dim_id].astype("string").fillna("")
    df["series_key"] = df[dsd_dim_ids].agg(".".join, axis=1)

    raw_codelists = extract_raw_codelists(dsd, structure_msg)
    label_maps = resolve_codelists(raw_codelists, ("en",))

    df["title"] = df.apply(
        lambda row: compose_observation_title(
            {dim_id: str(row.get(dim_id, "")).strip() for dim_id in dsd_dim_ids},
            dsd_dim_ids,
            label_maps,
        ),
        axis=1,
    )
    empty_title_mask = df["title"].astype(str).str.strip() == ""
    if empty_title_mask.any():
        df.loc[empty_title_mask, "title"] = df.loc[empty_title_mask, "series_key"]

    for dim_id in dsd_dim_ids:
        dim_labels = label_maps.get(dim_id, {})
        df[dim_id] = df[dim_id].map(
            lambda code, _labels=dim_labels: format_code_with_label(
                str(code), _labels.get(str(code))
            )
        )

    long_df = df[["series_key", "title", *dsd_dim_ids, "TIME_PERIOD", "value"]]

    additional_metadata: list[dict[str, str]] = []
    series_url = build_sdmx_dataset_url(agency_id, dataset_id)
    if series_url:
        additional_metadata.append({"name": "series_url", "value": series_url})

    prov = Provenance(
        source="sdmx",
        params={
            "dataset_key": params.dataset_key,
            "series_key": params.series_key,
            "start_period": params.start_period,
            "end_period": params.end_period,
        },
        properties={"metadata": additional_metadata} if additional_metadata else {},
    )
    ns = _sdmx_namespace_from_dataset_key(params.dataset_key)
    return _sdmx_fetch_output(ns, dsd_dim_ids).build_table_result(
        long_df,
        provenance=prov,
        params=params.model_dump(),
    )
