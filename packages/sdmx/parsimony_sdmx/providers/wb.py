"""World Bank (WB_WDI) adapter — custom path × decade discovery.

Unlike ECB/ESTAT/IMF, WB's SDMX endpoint does not return a series-keys
stream via ``sdmx1.series_keys``. We discover series by issuing a
Cartesian product of partial-key paths × decade windows against
``api.worldbank.org/v2/sdmx/rest/data/{flow}/...?detail=serieskeysonly``.

Key design choices preserved from the plan:

* ``ThreadPoolExecutor`` for parallelism; worker count is configurable
  and shares the subprocess-local ``requests.Session``.
* ``HTTP 404`` is a **successful empty** result, not retried, not
  logged at WARN. It's the API's way of saying "no data for that
  decade × path combination".
* Only non-404 4xx and 5xx flow through the retry machinery (handled
  by the shared session's ``HTTPAdapter``).

Structure queries (dataflow listing, dataflow+DSD fetch) bypass
``sdmx1`` entirely: the library's built-in URL pattern appends
``/latest`` (e.g. ``dataflow/WB/WDI/latest``), which WB's gateway
307-redirects to a deprecated ``http://dataapi.worldbank.org`` host
that answers ``403``. Direct HTTPS calls to the non-``/latest`` form
work; we fetch raw bytes and hand them to :func:`sdmx.read_sdmx` so
the resulting message has the same shape as ``client.dataflow()``.
"""

from __future__ import annotations

import io
import itertools
import logging
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import requests
import sdmx

from parsimony_sdmx.core.codelists import resolve_codelists
from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord
from parsimony_sdmx.core.projection import project_series
from parsimony_sdmx.io.http import HttpConfig, bounded_get, build_session
from parsimony_sdmx.io.xml import iter_elements
from parsimony_sdmx.providers.sdmx_extract import (
    extract_dsd_dim_order,
    extract_flow_title,
    extract_raw_codelists,
)
from parsimony_sdmx.providers.sdmx_flow import resolve_dsd

logger = logging.getLogger(__name__)

GENERIC_NS = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
SERIES_TAG = f"{{{GENERIC_NS}}}Series"
SERIES_KEY_TAG = f"{{{GENERIC_NS}}}SeriesKey"
VALUE_TAG = f"{{{GENERIC_NS}}}Value"

WB_BASE_URL = "https://api.worldbank.org/v2/sdmx/rest"
WB_TITLE_PREFIX = "World Bank - "

# WB's SDMX gateway is registered in sdmx1 under agency id ``WB``. Our
# internal agency id is ``WB_WDI`` so we can disambiguate from the WITS
# endpoint (``WB``) that ships with sdmx1.
WB_SDMX_AGENCY = "WB"


@dataclass(frozen=True, slots=True)
class WbConfig:
    max_workers: int = 10
    max_base_dims: int = 2
    decade_start: int = 1950
    decade_end: int = 2030
    decade_step: int = 10


@dataclass(frozen=True, slots=True)
class WbProvider:
    agency_id: str = "WB_WDI"
    http_config: HttpConfig = field(default_factory=HttpConfig)
    wb_config: WbConfig = field(default_factory=WbConfig)
    base_url: str = WB_BASE_URL

    def list_datasets(self) -> Iterator[DatasetRecord]:
        msg = _fetch_wb_structure(
            self.http_config, self.base_url, f"dataflow/{WB_SDMX_AGENCY}"
        )
        dataflows = getattr(msg, "dataflow", {}) or {}
        for flow_id, flow in dataflows.items():
            base_title = extract_flow_title(flow, ("en",))
            title = (
                f"{WB_TITLE_PREFIX}{base_title}"
                if base_title
                else WB_TITLE_PREFIX.rstrip(" -")
            )
            yield DatasetRecord(
                dataset_id=flow_id,
                agency_id=self.agency_id,
                title=title,
            )

    def list_series(self, dataset_id: str) -> Iterator[SeriesRecord]:
        msg = _fetch_wb_structure(
            self.http_config,
            self.base_url,
            f"dataflow/{WB_SDMX_AGENCY}/{dataset_id}?references=descendants",
        )
        try:
            dataflow = msg.dataflow[dataset_id]
        except (KeyError, AttributeError, TypeError) as exc:
            raise SdmxFetchError(
                f"Dataflow {dataset_id!r} missing from response"
            ) from exc
        dsd = resolve_dsd(_NoFetchClient(), msg, dataflow, dataset_id)
        dsd_order = extract_dsd_dim_order(dsd, exclude_time=True)
        raw_codelists = extract_raw_codelists(dsd, msg)
        labels = resolve_codelists(raw_codelists, ("en",))

        if not dsd_order:
            raise SdmxFetchError(
                f"WB dataset {dataset_id!r} has no non-time dimensions to sweep"
            )

        dim_codes = _codes_per_dim(raw_codelists, dsd_order)
        session = build_session(self.http_config)
        try:
            series_ids = discover_wb_series(
                session=session,
                base_url=self.base_url,
                dataset_id=dataset_id,
                dim_order=dsd_order,
                dim_codes=dim_codes,
                http_config=self.http_config,
                wb_config=self.wb_config,
            )
        finally:
            session.close()

        series_dim_values = (_split_to_dict(sid, dsd_order) for sid in sorted(series_ids))
        yield from project_series(
            dataset_id=dataset_id,
            series_dim_values=series_dim_values,
            dsd_order=dsd_order,
            labels=labels,
        )


def _fetch_wb_structure(
    http_config: HttpConfig, base_url: str, path: str
) -> Any:
    """Fetch a WB SDMX structure resource directly and parse it.

    Bypasses ``sdmx1.Client`` because the library appends ``/latest``
    to URLs, and WB's gateway 307-redirects ``/latest`` to a deprecated
    HTTP host that returns 403. The ``sdmx.read_sdmx`` output has the
    same message shape (``dataflow``, ``structure``, ``codelist``) that
    :mod:`parsimony_sdmx.providers.sdmx_extract` expects.
    """
    url = f"{base_url}/{path}"
    session = build_session(http_config)
    try:
        try:
            body = bounded_get(session, url, http_config)
        except SdmxFetchError as exc:
            raise SdmxFetchError(f"WB structure fetch {url}: {exc}") from exc
    finally:
        session.close()
    try:
        return sdmx.read_sdmx(io.BytesIO(body))
    except Exception as exc:
        raise SdmxFetchError(
            f"Failed to parse WB structure response from {url}: {exc}"
        ) from exc


class _NoFetchClient:
    """Sentinel client that refuses DSD lookups.

    The WB ``references=descendants`` response already embeds every DSD
    we need, so :func:`resolve_dsd` never touches the client. Passing an
    explicit stub instead of ``None`` makes the contract obvious: if this
    method ever fires, our assumption about WB's response shape has broken
    and we want the loud failure.
    """

    def datastructure(self, **_kwargs: Any) -> Any:
        raise SdmxFetchError(
            "WB DSD lookup required but unavailable — "
            "expected response to embed the DSD via references=descendants"
        )


def discover_wb_series(
    session: requests.Session,
    base_url: str,
    dataset_id: str,
    dim_order: Sequence[str],
    dim_codes: dict[str, list[str]],
    http_config: HttpConfig,
    wb_config: WbConfig,
) -> set[str]:
    """Parallel path × decade sweep. Returns the set of discovered series IDs."""
    decades = _build_decades(wb_config)
    paths = _build_path_combinations(dim_order, dim_codes, wb_config.max_base_dims)
    tasks = [(path, decade) for path in paths for decade in decades]
    logger.info(
        "WB discovery: %d paths × %d decades = %d requests for %s",
        len(paths),
        len(decades),
        len(tasks),
        dataset_id,
    )

    series_ids: set[str] = set()

    def fetch(task: tuple[str, tuple[str, str]]) -> set[str]:
        path, (start, end) = task
        url = (
            f"{base_url}/data/{dataset_id}/{path}/"
            f"?startPeriod={start}&endPeriod={end}&detail=serieskeysonly"
        )
        return _fetch_path_decade(session, url, http_config, dim_order)

    with ThreadPoolExecutor(max_workers=wb_config.max_workers) as pool:
        for keys in pool.map(fetch, tasks):
            series_ids.update(keys)

    logger.info(
        "WB discovery for %s yielded %d unique series", dataset_id, len(series_ids)
    )
    return series_ids


def _build_decades(wb_config: WbConfig) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for y in range(wb_config.decade_start, wb_config.decade_end, wb_config.decade_step):
        out.append((str(y), str(y + wb_config.decade_step - 1)))
    return out


def _build_path_combinations(
    dim_order: Sequence[str],
    dim_codes: dict[str, list[str]],
    max_base_dims: int,
) -> list[str]:
    if not dim_order:
        return []
    base_dims = list(dim_order[:max_base_dims])
    wildcard_count = max(len(dim_order) - len(base_dims), 0)
    code_lists = [dim_codes.get(d, []) for d in base_dims]
    if not all(code_lists):
        # Some base dim has no known codes — fall back to a single wildcard path.
        return [".".join([""] * len(dim_order))]
    return [
        ".".join(list(combo) + [""] * wildcard_count)
        for combo in itertools.product(*code_lists)
    ]


def _codes_per_dim(
    raw_codelists: dict[str, dict[str, dict[str, str]]],
    dim_order: Sequence[str],
) -> dict[str, list[str]]:
    return {dim: list(raw_codelists.get(dim, {}).keys()) for dim in dim_order}


def _fetch_path_decade(
    session: requests.Session,
    url: str,
    config: HttpConfig,
    dim_order: Sequence[str],
) -> set[str]:
    """Fetch one path × decade URL.

    Only ``HTTP 404`` is treated as a legitimate "empty decade"; anything
    else (including network errors and non-404 4xx/5xx after the session
    adapter's retries) propagates as :class:`SdmxFetchError` so the worker
    classifies the dataset as FAILED rather than silently returning an
    empty catalog. See review finding #11.
    """
    try:
        response = session.get(
            url,
            timeout=config.timeout,
            stream=True,
        )
    except requests.RequestException as exc:
        raise SdmxFetchError(
            f"WB fetch network error for {url}: {exc}"
        ) from exc

    try:
        if response.status_code == 404:
            return set()
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise SdmxFetchError(
                f"WB fetch {url} returned HTTP {response.status_code}"
            ) from exc
        content = _read_bounded(response, config)
        return _parse_series_ids(content, dim_order)
    finally:
        response.close()


def _read_bounded(response: requests.Response, config: HttpConfig) -> bytes:
    """Read the response body with a hard byte cap."""
    cap = config.max_response_bytes
    buf = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if not chunk:
            continue
        remaining = cap - len(buf)
        if remaining <= 0 or len(chunk) > remaining:
            raise SdmxFetchError(
                f"WB response from {response.url} exceeded {cap} bytes"
            )
        buf.extend(chunk)
    return bytes(buf)


def _parse_series_ids(xml_bytes: bytes, dim_order: Sequence[str]) -> set[str]:
    result: set[str] = set()
    try:
        for series_elem in iter_elements(xml_bytes, SERIES_TAG):
            key_elem = series_elem.find(SERIES_KEY_TAG)
            if key_elem is None:
                continue
            key_values: dict[str, str] = {}
            for v in key_elem.findall(VALUE_TAG):
                dim_id = v.get("id")
                code = v.get("value")
                if dim_id and code:
                    key_values[dim_id] = code
            parts: list[str] = []
            for dim in dim_order:
                val = key_values.get(dim)
                if val is None:
                    parts = []
                    break
                parts.append(val)
            if parts:
                result.add(".".join(parts))
    except SdmxFetchError:
        logger.debug("Skipping malformed WB response chunk")
    return result


def _split_to_dict(series_id: str, dim_order: Sequence[str]) -> dict[str, str]:
    parts = series_id.split(".")
    if len(parts) != len(dim_order):
        return {}
    return dict(zip(dim_order, parts, strict=True))
