"""``sdmx_fetch`` — live SDMX retrieval connector.

Performs strict param validation against a closed agency allowlist,
budgets the SDMX round-trip with a thread-pool timeout,
and applies bounded retries on transient transport failures.

The body imports ``sdmx`` and ``pandas`` lazily so that just importing
``parsimony_sdmx`` to inspect ``CONNECTORS`` does not drag ``sdmx1`` into the parent
process — guarded by ``tests/test_listing.py::test_plugin_surface_import_does_not_pull_sdmx``.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, TypeVar

from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, ProviderError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputSpec,
)
from pydantic import BaseModel, Field, field_validator
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout

from parsimony_sdmx.core.agencies import ALL_AGENCIES
from parsimony_sdmx.core.titles import compose_observation_title
from parsimony_sdmx.providers.dataset_urls import build_sdmx_dataset_url
from parsimony_sdmx.series_fields import dim_code_field, dim_label_field

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2
_RETRY_BASE_DELAY_SEC = 0.5
_RETRY_MAX_DELAY_SEC = 4.0
_FETCH_TIMEOUT_SEC = 45.0
#: Upper bound on keys per batched ``sdmx_fetch`` call, and how many of them run at once.
#: The cap keeps a single call from fanning out an unbounded request storm at one provider;
#: the worker count keeps concurrency gentle while still collapsing a several-series fetch
#: from sum-of-latencies down to slowest-single-latency.
_MAX_BATCH_SERIES = 24
_MAX_FETCH_WORKERS = 6
#: Hard cap on the length of a single ``series_ref`` string (one key, which may carry ``+``
#: OR-lists within a dimension). SDMX's REST path has a practical URL-length limit; past this
#: a caller splits the pull into several ``<=256``-char OR-strings and passes them as a list.
_SERIES_KEY_MAX_CHARS = 256

_T = TypeVar("_T")

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
            max_length=_SERIES_KEY_MAX_CHARS,
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
            raise InvalidParameterError("sdmx", "dataset_ref must include agency prefix (e.g. 'ECB-YC')")
        agency, dataset_id = stripped.split("-", 1)
        allowed = {a.value for a in ALL_AGENCIES}
        if agency.upper() not in allowed:
            raise InvalidParameterError("sdmx", f"Unknown agency {agency!r}; allowed: {sorted(allowed)}")
        import re

        if not re.match(_DATASET_KEY_PATTERN, dataset_id):
            raise InvalidParameterError("sdmx", f"dataset_id {dataset_id!r} contains disallowed characters")
        return f"{agency.upper()}-{dataset_id}"


@dataclass(frozen=True)
class _ResolvedStructure:
    """DSD + codelists for one dataset — resolved once, shared across every key in a same-flow batch.

    ``sdmx_fetch``'s per-series worker only depends on this for its dimension order and
    code→label maps; it never depends on ``series_key``, so every key in one call can share
    a single instance instead of each re-fetching and re-parsing it (see :func:`_resolve_structure`).
    """

    dataset_id: str
    dsd: Any
    structure_msg: Any
    dsd_dim_ids: list[str]
    label_maps: dict[str, dict[str, str]]


# Static tabular schema for a fetch. Each per-flow dimension is emitted as a bare
# ``{dim}_code`` column (the clean code, matching sdmx_series_search's code fields);
# the human labels ride in ``title``. UNIT / UNIT_MULT additionally carry a ``_label``
# because their meaning is not in the title. Those per-flow columns (plus the optional
# series_url) vary by flow, so they are caught by the ``"*"`` wildcard as METADATA
# rather than enumerated. series_key carries no namespace,
# matching sdmx_series_search's key column (the join target).
# TIME_PERIOD stays the raw SDMX period label (``2020`` / ``2020-Q1`` / ``2020-01``
# / ``2020-01-01``), NOT coerced to datetime: granularity rides on the flow's FREQ
# dimension and a key list can mix frequencies, so there is no honest single-instant
# form — coercing would fabricate precision and outright fails on quarterly/weekly
# labels. Consumers parse to a datetime axis on demand (``pd.PeriodIndex``).
SDMX_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_key", role=ColumnRole.KEY),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="TIME_PERIOD", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
        Column(name="*", role=ColumnRole.METADATA),
    ]
)


def _is_retryable(exc: BaseException) -> bool:
    """Transient errors worth a bounded retry: connect / timeout / 5xx / 429."""
    if isinstance(exc, (Timeout, RequestsConnectionError)):
        return True
    if isinstance(exc, HTTPError):
        status = getattr(getattr(exc, "response", None), "status_code", 0)
        return status in (502, 503, 504) or status == 429
    return False


def _dataset_id_from_ref(dataset_ref: str) -> str | None:
    """Best-effort ``dataset_id`` half of ``AGENCY-DATASET_ID``; ``None`` if malformed.

    Used only for the defensive flow-prefix strip below — real validation of the whole
    ``dataset_ref`` happens in :class:`SdmxFetchParams`, so a malformed ref just falls through
    to that validator's proper error instead of failing here.
    """
    if "-" not in dataset_ref:
        return None
    return dataset_ref.split("-", 1)[1] or None


def _is_empty_document_error(exc: BaseException) -> bool:
    """A no-data 200 response: the provider sent an empty body, so XML parsing dies.

    Observed on ECB when ``startPeriod``/``endPeriod`` fall outside a series' coverage
    (e.g. a flow discontinued in 2014 queried for 2023+): HTTP 200, zero bytes, and
    sdmx1's lxml parser raises ``XMLSyntaxError("no element found ...")``. That outcome
    is deterministic — it must surface as :class:`EmptyDataError` (adjust the period
    range), never as a "transient, retry shortly" :class:`ProviderError`. Matched by
    type name so lxml stays a transitive dependency.
    """
    return type(exc).__name__ == "XMLSyntaxError" and "no element found" in str(exc)


def _strip_flow_prefix(key: str, dataset_id: str) -> str:
    """Strip a redundant leading ``<dataset_id>.`` flow prefix from a caller-supplied key.

    ``sdmx_series_search``'s ``key`` column is documented to paste directly into ``series_ref``
    (discover → search → fetch); older catalogs (or a key copy-pasted from a provider's raw
    SDMX-CSV ``KEY`` column, which some agencies — observed on ECB — prefix with the flow id)
    can still carry that prefix. ``sdmx_fetch`` wants the bare, unprefixed key: passing the
    prefixed form straight through 400s at the provider (duplicated flow id in the URL path).
    Case-insensitive since ECB/IMF request the flow uppercased but don't guarantee the export
    echoes that same case back.
    """
    prefix, sep, rest = key.partition(".")
    return rest if sep and prefix.upper() == dataset_id.upper() else key


def _check_or_group_coverage(params: SdmxFetchParams, dsd_dim_ids: list[str], df: Any) -> None:
    """Raise when a ``+``-OR'd code contributed zero observations to a wide pull.

    A ``+``-joined pull returns whatever subset the provider has: a member with no
    observations in the requested window simply vanishes from the frame (observed live —
    UK dropped from an ``EL+TR+IS+UK`` Eurostat HICP pull with no signal, because the UK
    stopped reporting after 2020). The connector promises none-dropped semantics for key
    lists; this extends it to OR groups. Checked per dimension: an unpopulated
    cross-product of two OR groups (CP02 x FR missing while CP02 and FR each return
    rows) is not detectable this way — same granularity as the search-side diagnostics.
    """
    segments = params.series_key.split(".")
    if len(segments) != len(dsd_dim_ids):
        # Partial or oddly-shaped keys: leave the provider's own semantics alone.
        return
    problems: list[str] = []
    for dim_id, segment in zip(dsd_dim_ids, segments, strict=True):
        requested = {value for value in segment.split("+") if value}
        if len(requested) < 2:
            # A single (or wildcarded) value can't be silently dropped: if it matched
            # nothing the whole frame is empty, which already raises EmptyDataError.
            continue
        returned = set(df[dim_id].astype(str))
        missing = sorted(requested - returned)
        if missing:
            problems.append(f"{dim_id}: {missing}")
    if problems:
        period = f"{params.start_period or '(open)'}..{params.end_period or '(open)'}"
        raise EmptyDataError(
            provider="sdmx",
            message=(
                f"'+'-OR values returned no observations in {params.dataset_key} over {period}: "
                + "; ".join(problems)
                + ". The other requested codes have data — drop the missing codes from the "
                "OR-string or widen start_period/end_period, then re-fetch."
            ),
        )


@connector(output=SDMX_FETCH_OUTPUT, tags=["sdmx"])
def sdmx_fetch(
    dataset_ref: str,
    series_ref: str | list[str],
    start_period: str | None = None,
    end_period: str | None = None,
) -> Any:
    """Fetch observations for SDMX series of ONE flow.

    dataset_ref is the flow as AGENCY-DATASET_ID (e.g. ECB-YC), from sdmx_datasets_search.
    series_ref is one or more keys; dimensions are positional in DSD order (paste
    sdmx_series_search's `key` straight in).

    FAST wide pull: one '+'-joined OR-string. SDMX reads '+' as OR *within a dimension*, so
    "M.CP01+CP02.DE+FR" fetches the cross-product in ONE round trip — cheaper than a key list.
    A string is capped at 256 chars; past that, pass several <=256-char
    OR-strings as a LIST (capped at 24, fetched concurrently). An empty dimension wildcards it
    (slow); prefer sdmx_series_search then fetch by key. start/end_period filter each range.

    Raises ProviderError/EmptyDataError/ParseError if any key fails (none dropped).
    """
    keys = series_ref if isinstance(series_ref, list) else [series_ref]
    if not keys:
        raise InvalidParameterError("sdmx", "series_ref must name at least one series key")
    if len(keys) > _MAX_BATCH_SERIES:
        raise InvalidParameterError(
            "sdmx",
            f"series_ref accepts at most {_MAX_BATCH_SERIES} keys per list; got {len(keys)}. "
            "For a wider pull, OR-list codes within a dimension using '+' in a single key "
            "(e.g. 'M.RCH_A.CP01+CP02+CP03.DE+FR+IT') — SDMX treats '+' as OR within a dimension, "
            "so one such string fetches the whole cross-product in one round trip. To fetch more "
            f"than that spans, split into multiple <={_SERIES_KEY_MAX_CHARS}-char OR-strings and "
            f"pass up to {_MAX_BATCH_SERIES} of them as a list.",
        )

    dataset_id = _dataset_id_from_ref(dataset_ref)
    if dataset_id:
        keys = [_strip_flow_prefix(key, dataset_id) for key in keys]

    over = next((key for key in keys if len(key) > _SERIES_KEY_MAX_CHARS), None)
    if over is not None:
        raise InvalidParameterError(
            "sdmx",
            f"a single series_ref string is capped at {_SERIES_KEY_MAX_CHARS} chars (got "
            f"{len(over)}); split it into multiple <={_SERIES_KEY_MAX_CHARS}-char '+'-joined "
            f"OR-strings and pass them as a list (up to {_MAX_BATCH_SERIES} items) to fetch them "
            "together.",
        )

    param_list = [
        SdmxFetchParams(
            dataset_key=dataset_ref,
            series_key=key,
            start_period=start_period,
            end_period=end_period,
        )
        for key in keys
    ]

    structure = _resolve_structure(param_list[0].dataset_key)

    if len(param_list) == 1:
        return _fetch_one_series(param_list[0], structure)

    import pandas as pd

    frames = _fetch_series_concurrently(param_list, structure)
    return pd.concat(frames, ignore_index=True)


def _fetch_series_concurrently(param_list: list[SdmxFetchParams], structure: _ResolvedStructure) -> list[Any]:
    """Fetch each series of a same-flow batch on its own thread, preserving request order.

    Every key keeps its own per-series timeout budget and transient-retry handling, so one slow
    round-trip cannot drag the others; the first failure (in request order) propagates, making the
    batch all-or-nothing so no requested key is silently dropped. All keys share the one already-
    resolved *structure* — see :func:`_resolve_structure`.
    """
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial

    worker = partial(_fetch_one_series, structure=structure)
    with ThreadPoolExecutor(max_workers=min(_MAX_FETCH_WORKERS, len(param_list))) as pool:
        return list(pool.map(worker, param_list))


def _run_budgeted(fn: Callable[[], _T], *, op_label: str, hint_fn: Callable[[int, str], str]) -> _T:
    """Shared timeout + bounded-retry policy for one budgeted SDMX network operation.

    Used by both the per-series data fetch and the once-per-batch structure resolution — same
    transport-failure handling and retry policy; *hint_fn* tailors the final error framing
    (``(status_code, detail) -> hint text``) to whichever operation failed.
    """
    from concurrent.futures import ThreadPoolExecutor
    from concurrent.futures import TimeoutError as FuturesTimeoutError

    attempt = 0
    while True:
        attempt += 1
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(fn)
                try:
                    return future.result(timeout=_FETCH_TIMEOUT_SEC)
                except FuturesTimeoutError as exc:
                    raise TimeoutError from exc
        except (ProviderError, EmptyDataError, ParseError):
            raise
        except TimeoutError as exc:
            raise ProviderError(
                provider="sdmx",
                status_code=0,
                message=(
                    f"SDMX fetch exceeded {_FETCH_TIMEOUT_SEC:.0f}s budget for {op_label}. "
                    "Narrow the pull and retry: bound start_period/end_period and OR fewer "
                    "codes per '+' group — unbounded wide pulls are the usual cause."
                ),
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
                time.sleep(delay)
                continue
            status = 0
            if isinstance(exc, HTTPError):
                status = getattr(getattr(exc, "response", None), "status_code", 0) or 0
            detail = (str(exc).strip() or type(exc).__name__)[:200]
            raise ProviderError(
                provider="sdmx",
                status_code=status,
                message=f"SDMX fetch failed for {op_label}: {hint_fn(status, detail)}",
            ) from exc


def _fetch_one_series(params: SdmxFetchParams, structure: _ResolvedStructure) -> Any:
    """Fetch ONE series' observations against an already-resolved *structure* (see :func:`_resolve_structure`)."""

    def _hint(status: int, detail: str) -> str:
        if 400 <= status < 500 and status != 429:
            return (
                f"HTTP {status}: the provider rejected the key — a code in "
                f"{params.series_key!r} is not valid or not populated for this flow. This is a bad-key "
                "error, not a network failure: re-check each dimension's code against the DSD "
                "(sdmx_dimension_search lists a dimension's valid codes), then retry. The "
                "flow is reachable; do not switch flows or providers on this alone."
            )
        return (
            f"HTTP {status or 'n/a'}: transient fetch error after {_MAX_RETRIES + 1} attempts "
            f"({detail}). Retry shortly, or move on to a sibling flow if it persists."
        )

    return _run_budgeted(
        lambda: _do_sdmx_fetch(params, structure),
        op_label=f"{params.dataset_key}/{params.series_key}",
        hint_fn=_hint,
    )


def _resolve_structure(dataset_key: str) -> _ResolvedStructure:
    """Resolve the DSD + codelists for *dataset_key*, with the same retry/timeout budget as a series fetch.

    Hoisted out of the per-series worker: every key in one ``sdmx_fetch(series_ref=[...])`` call
    depends on the exact same structure (it's a function of ``dataset_key`` alone), so having each
    key independently re-fetch and re-parse the whole DSD + every codelist was pure waste — and
    pathological on flows with large codelists (e.g. ECB's ``YC``, 1000+ codes in one dimension).
    """

    def _hint(status: int, detail: str) -> str:
        if 400 <= status < 500 and status != 429:
            return (
                f"HTTP {status}: the provider rejected dataset {dataset_key!r}'s structure request — "
                "re-check dataset_ref against the agency's dataflow list."
            )
        return (
            f"HTTP {status or 'n/a'}: transient structure-fetch error after {_MAX_RETRIES + 1} attempts "
            f"({detail}). Retry shortly, or move on to a sibling flow if it persists."
        )

    return _run_budgeted(
        lambda: _fetch_structure(dataset_key),
        op_label=f"structure/{dataset_key}",
        hint_fn=_hint,
    )


def _fetch_structure(dataset_key: str) -> _ResolvedStructure:
    """Single-attempt structure fetch — see :func:`_resolve_structure` for the retry wrapper.

    Imports ``sdmx`` and the provider helpers function-locally to keep the parent
    ``parsimony_sdmx`` import graph free of ``sdmx1``.
    """
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

    agency_id, dataset_id = dataset_key.split("-", 1)

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
        except HTTPError:
            raise
        except SdmxFetchError as exc:
            cause = exc.__cause__
            if isinstance(cause, HTTPError):
                raise cause from exc
            raise ProviderError(
                provider="sdmx",
                status_code=0,
                message=f"Failed to fetch structure for {dataset_id}: {(str(exc).strip() or type(exc).__name__)[:200]}",
            ) from exc

    dsd_dim_ids = extract_dsd_dim_order(dsd, exclude_time=True)
    if not dsd_dim_ids:
        raise ParseError(
            provider="sdmx",
            message="Unable to determine SDMX series dimensions for series_key",
        )

    raw_codelists = extract_raw_codelists(dsd, structure_msg)
    label_maps = resolve_codelists(raw_codelists, ("en",))

    return _ResolvedStructure(
        dataset_id=dataset_id,
        dsd=dsd,
        structure_msg=structure_msg,
        dsd_dim_ids=dsd_dim_ids,
        label_maps=label_maps,
    )


def _do_sdmx_fetch(params: SdmxFetchParams, structure: _ResolvedStructure) -> Any:
    """Inner fetch — issues the data-only SDMX request and shapes the observation table.

    ``structure`` (DSD + codelists) is pre-resolved by :func:`_resolve_structure` and shared
    across the whole batch; this only performs the per-series ``data`` request and projection.
    Imports ``sdmx`` / ``pandas`` function-locally to keep the parent ``parsimony_sdmx`` import
    graph free of ``sdmx1``.
    """
    import pandas as pd
    import sdmx as sdmx_lib

    from parsimony_sdmx.providers.sdmx_client import sdmx_client

    agency_id, dataset_id = params.dataset_key.split("-", 1)

    with sdmx_client(agency_id, wb_url_rewrite=True) as client:
        try:
            data_msg = client.get(
                resource_type="data",
                resource_id=dataset_id,
                key=params.series_key,
                params={
                    "startPeriod": params.start_period,
                    "endPeriod": params.end_period,
                },
            )
        except Exception as exc:
            if _is_empty_document_error(exc):
                period = f"{params.start_period or '(open)'}..{params.end_period or '(open)'}"
                raise EmptyDataError(
                    provider="sdmx",
                    message=(
                        f"No observations for {params.series_key!r} in {params.dataset_key} over "
                        f"{period}: the provider returned an empty document. This is deterministic, "
                        "not transient — widen or drop start_period/end_period; if it persists with "
                        "no period filter, the series is likely discontinued at the source."
                    ),
                ) from exc
            raise

    # Request dataset/group/series-level attributes so unit metadata (UNIT, UNIT_MULT)
    # rides along where the agency provides it (ECB does; ESTAT models unit as a
    # dimension instead). Attribute support varies by agency/message shape, so fall
    # back to the plain values-only conversion rather than failing the fetch over it.
    try:
        raw = sdmx_lib.to_pandas(data_msg.data, attributes="dgs")
    except Exception as exc:
        logger.debug("sdmx_fetch: attribute conversion failed (%s); retrying values-only", exc)
        raw = sdmx_lib.to_pandas(data_msg.data)
    df = raw.rename("value").to_frame().reset_index() if isinstance(raw, pd.Series) else pd.DataFrame(raw).reset_index()
    if df.empty:
        raise EmptyDataError(provider="sdmx", message="No data returned for requested series.")

    if "value" not in df.columns:
        value_columns = [col for col in df.columns if col not in {"TIME_PERIOD"}]
        if len(value_columns) != 1:
            raise ParseError(provider="sdmx", message="Unable to determine SDMX value column")
        df = df.rename(columns={value_columns[0]: "value"})
    # With attributes requested, sdmx1 may deliver values as object dtype; the declared
    # output dtype is numeric, so coerce (unparseable observations become NaN, the same
    # as a missing observation).
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    dsd_dim_ids = structure.dsd_dim_ids
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
    # Run on raw codes, before labels are folded into the dimension columns.
    _check_or_group_coverage(params, dsd_dim_ids, df)
    df["series_key"] = df[dsd_dim_ids].agg(".".join, axis=1)

    label_maps = structure.label_maps
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

    # Emit each dimension as a bare {dim}_code column — the clean code, matching
    # sdmx_series_search's code fields, usable directly for filter/groupby/re-fetch
    # instead of a "CODE (Label)" display string. The human labels already ride in
    # `title` (the dimension labels concatenated), so a per-dimension label column
    # would only restate them.
    for dim_id in dsd_dim_ids:
        df[dim_code_field(dim_id)] = df[dim_id].astype(str)

    # Unit attributes (when the agency provides them) qualify what `value` means and,
    # unlike the dimensions, are NOT carried by `title` — so keep both the code and its
    # label. Anything else attribute-shaped (OBS_STATUS, TITLE_COMPL, ...) is dropped to
    # keep the frame lean.
    unit_cols = [col for col in ("UNIT", "UNIT_MULT") if col in df.columns]
    for col in unit_cols:
        unit_labels = label_maps.get(col, {})
        codes = df[col].astype(str)
        df[dim_code_field(col)] = codes
        df[dim_label_field(col)] = codes.map(lambda code, _labels=unit_labels: _labels.get(code, code))

    dim_out = [dim_code_field(dim_id) for dim_id in dsd_dim_ids]
    unit_out = [c for col in unit_cols for c in (dim_code_field(col), dim_label_field(col))]
    # Declared columns lead in spec order; per-flow metadata columns trail.
    long_df = df[["series_key", "title", "TIME_PERIOD", "value", *dim_out, *unit_out]].copy()

    series_url = build_sdmx_dataset_url(agency_id, dataset_id)
    if series_url:
        long_df["series_url"] = series_url
    return long_df
