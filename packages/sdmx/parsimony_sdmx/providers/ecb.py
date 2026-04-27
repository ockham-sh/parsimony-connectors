"""ECB adapter — shared SDMX flow + per-series TITLE/TITLE_COMPL + portal scrape."""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord
from parsimony_sdmx.core.titles import augment_with_ecb_attributes
from parsimony_sdmx.io.http import HttpConfig, bounded_get, build_session
from parsimony_sdmx.providers.ecb_portal import scrape_ecb_portal
from parsimony_sdmx.providers.ecb_series_attrs import parse_ecb_series_attributes
from parsimony_sdmx.providers.sdmx_client import sdmx_client
from parsimony_sdmx.providers.sdmx_extract import extract_dsd_dim_order
from parsimony_sdmx.providers.sdmx_flow import (
    fetch_dataflow_with_structure,
    list_datasets_flow,
    list_series_flow,
    resolve_dsd,
)

logger = logging.getLogger(__name__)

ECB_SERIES_ATTRS_URL = "https://data-api.ecb.europa.eu/service/data/{flow_id}?detail=nodata"


@dataclass(frozen=True, slots=True)
class EcbProvider:
    agency_id: str = "ECB"
    cache_dir: Path | None = None
    http_config: HttpConfig = field(default_factory=HttpConfig)

    def list_datasets(self) -> Iterator[DatasetRecord]:
        session = build_session(self.http_config)
        try:
            descriptions = scrape_ecb_portal(
                session,
                cache_dir=self.cache_dir,
                http_config=self.http_config,
            )
        finally:
            session.close()

        def decorate(flow_id: str, base_title: str) -> str:
            extra = descriptions.get(flow_id)
            if extra:
                return f"{base_title}. {extra}" if base_title else extra
            return base_title

        with sdmx_client(self.agency_id, self.http_config) as client:
            yield from list_datasets_flow(
                client, self.agency_id, decorate_title=decorate
            )

    def list_series(self, dataset_id: str) -> Iterator[SeriesRecord]:
        with sdmx_client(self.agency_id, self.http_config) as client:
            # Resolve DSD dimension order up front so we can parse the
            # per-series XML in the correct column order before delegating
            # the series iteration to the shared flow.
            msg = fetch_dataflow_with_structure(client, dataset_id)
            try:
                dataflow = msg.dataflow[dataset_id]
            except (KeyError, AttributeError, TypeError) as exc:
                raise SdmxFetchError(
                    f"Dataflow {dataset_id!r} missing from response"
                ) from exc
            dsd = resolve_dsd(client, msg, dataflow, dataset_id)
            dsd_order = extract_dsd_dim_order(dsd, exclude_time=True)

            attrs_map = _fetch_series_attributes(
                dataset_id,
                dsd_order,
                self.http_config,
            )
            augment = _build_augment(attrs_map)
            augment_fragments = _build_augment_fragments(attrs_map)

            yield from list_series_flow(
                client,
                self.agency_id,
                dataset_id,
                augment=augment,
                augment_fragments=augment_fragments,
            )


def _fetch_series_attributes(
    dataset_id: str,
    dim_order: list[str],
    http_config: HttpConfig,
) -> dict[str, tuple[str | None, str | None]]:
    """Fetch the per-series XML and parse it. Failures return an empty map
    so the series title falls back to the bare codelist concatenation."""
    url = ECB_SERIES_ATTRS_URL.format(flow_id=dataset_id)
    # ECB XML responses can be huge; read-timeout stays tight so a hung
    # upstream doesn't hold a subprocess for the full dataset timeout.
    attrs_config = dataclasses.replace(
        http_config,
        read_timeout=max(30.0, http_config.read_timeout / 2),
    )
    session = build_session(attrs_config)
    try:
        xml_bytes = bounded_get(
            session,
            url,
            config=attrs_config,
            extra_headers={"Accept": "application/xml"},
        )
    except SdmxFetchError as exc:
        logger.warning(
            "ECB series attributes fetch failed for %s: %s — "
            "proceeding without TITLE/TITLE_COMPL",
            dataset_id,
            exc,
        )
        return {}
    finally:
        session.close()

    try:
        return parse_ecb_series_attributes(xml_bytes, dim_order)
    except SdmxFetchError as exc:
        logger.warning(
            "ECB series attributes parse failed for %s: %s — proceeding without",
            dataset_id,
            exc,
        )
        return {}


def _build_augment(
    attrs_map: dict[str, tuple[str | None, str | None]],
) -> Callable[[str, str], str]:
    """Build the ``(base, series_id) -> str`` hook for ``project_series``."""

    def augment(base: str, series_id: str) -> str:
        title, title_compl = attrs_map.get(series_id, (None, None))
        return augment_with_ecb_attributes(base, title, title_compl)

    return augment


def _build_augment_fragments(
    attrs_map: dict[str, tuple[str | None, str | None]],
) -> Callable[[str], tuple[str, ...]]:
    """Emit per-series ECB ``TITLE`` as an additional embedding fragment.

    Phase 3 escalation path #1 (see ``PLAN-sdmx-catalog-search.md`` §3):
    codelist labels alone don't bridge colloquial-vocabulary queries
    ("yen daily exchange rate") to the rich natural-language overlay
    that the ECB ``TITLE`` carries ("Japanese yen/Euro ECB reference
    exchange rate"). The full title is in the display string for BM25
    but the bag-of-fragments compose path only sees codelist labels —
    so the embedder loses access to the bridging vocabulary.

    Adding TITLE as one fragment per series restores parity with the
    pre-Phase-2 full-title embedding without resurrecting TITLE_COMPL
    (the user-rejected text that lived on under earlier variants).
    """

    def augment_fragments(series_id: str) -> tuple[str, ...]:
        title, _title_compl = attrs_map.get(series_id, (None, None))
        return (title,) if title else ()

    return augment_fragments
