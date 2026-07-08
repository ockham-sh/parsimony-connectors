"""World Bank (WB_WDI) adapter — direct structure fetch.

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
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import sdmx

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import DatasetRecord, StructureRecord
from parsimony_sdmx.io.http import HttpConfig, bounded_get, build_session
from parsimony_sdmx.providers.sdmx_extract import extract_flow_title
from parsimony_sdmx.providers.sdmx_flow import structure_from_message

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
        msg = _fetch_wb_structure(self.http_config, self.base_url, f"dataflow/{WB_SDMX_AGENCY}")
        dataflows = getattr(msg, "dataflow", {}) or {}
        for flow_id, flow in dataflows.items():
            base_title = extract_flow_title(flow, ("en",))
            title = f"{WB_TITLE_PREFIX}{base_title}" if base_title else WB_TITLE_PREFIX.rstrip(" -")
            yield DatasetRecord(
                dataset_id=flow_id,
                agency_id=self.agency_id,
                title=title,
            )

    def fetch_structure(self, dataset_id: str) -> StructureRecord:
        msg = _fetch_wb_structure(
            self.http_config,
            self.base_url,
            f"dataflow/{WB_SDMX_AGENCY}/{dataset_id}?references=descendants",
        )
        return structure_from_message(
            msg,
            _NoFetchClient(),
            agency_id=self.agency_id,
            dataset_id=dataset_id,
        )


def _fetch_wb_structure(http_config: HttpConfig, base_url: str, path: str) -> Any:
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
        raise SdmxFetchError(f"Failed to parse WB structure response from {url}: {exc}") from exc


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
            "WB DSD lookup required but unavailable — expected response to embed the DSD via references=descendants"
        )
