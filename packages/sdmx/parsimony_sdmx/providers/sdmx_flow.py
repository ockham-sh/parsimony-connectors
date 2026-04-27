"""Shared SDMX flow for agencies whose discovery uses ``sdmx1.series_keys``.

ESTAT, IMF_DATA, and ECB all follow the same two-call shape:

1. ``client.dataflow(resource_id=DATASET, params={references: descendants})``
   returns a structure message with the DSD and all codelists needed for
   the dataset.
2. ``client.series_keys(DATASET)`` returns a ``SeriesKey`` stream.

ECB additionally augments each series with TITLE / TITLE_COMPL via an
XML endpoint — passed here as the ``augment`` hook from T8.

WB diverges entirely and lives in its own module (T9).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from typing import Any

from parsimony_sdmx.core.codelists import resolve_codelists
from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord
from parsimony_sdmx.core.projection import (
    SeriesFragmentsAugment,
    SeriesIdAugment,
    project_series,
)
from parsimony_sdmx.providers.sdmx_extract import (
    extract_dsd_dim_order,
    extract_flow_title,
    extract_raw_codelists,
    extract_series_dim_values,
)

TitleDecorator = Callable[[str, str], str]
"""Hook called as ``decorator(dataflow_id, base_title) -> decorated_title``."""


def list_datasets_flow(
    client: Any,
    agency_id: str,
    language_prefs: Sequence[str] = ("en",),
    decorate_title: TitleDecorator | None = None,
) -> Iterator[DatasetRecord]:
    """Yield one ``DatasetRecord`` per dataflow exposed by ``client``.

    ``decorate_title`` allows agency-specific tweaks (ECB prefixes its
    portal description; WB prefixes ``"World Bank - "``). Return the
    final title string — the caller does no further composition.
    """
    try:
        msg = client.dataflow(force=True)
    except Exception as exc:
        raise SdmxFetchError(
            f"Failed to list dataflows for {agency_id}: {exc}"
        ) from exc

    dataflows = getattr(msg, "dataflow", {}) or {}
    for flow_id, flow in dataflows.items():
        title = extract_flow_title(flow, language_prefs)
        if decorate_title is not None:
            title = decorate_title(flow_id, title)
        yield DatasetRecord(
            dataset_id=flow_id,
            agency_id=agency_id,
            title=title,
        )


def list_series_flow(
    client: Any,
    agency_id: str,
    dataset_id: str,
    language_prefs: Sequence[str] = ("en",),
    augment: SeriesIdAugment | None = None,
    augment_fragments: SeriesFragmentsAugment | None = None,
) -> Iterator[SeriesRecord]:
    """Fetch DSD + codelists + series keys, yield ``SeriesRecord`` per series."""
    msg = fetch_dataflow_with_structure(client, dataset_id)

    try:
        dataflow = msg.dataflow[dataset_id]
    except (KeyError, AttributeError, TypeError) as exc:
        raise SdmxFetchError(
            f"Dataflow {dataset_id!r} missing from response for {agency_id}"
        ) from exc

    dsd = resolve_dsd(client, msg, dataflow, dataset_id)
    dsd_order = extract_dsd_dim_order(dsd, exclude_time=True)
    raw_codelists = extract_raw_codelists(dsd, msg)
    labels = resolve_codelists(raw_codelists, language_prefs)

    try:
        series_keys = client.series_keys(dataset_id)
    except Exception as exc:
        raise SdmxFetchError(
            f"Failed to fetch series keys for {dataset_id}: {exc}"
        ) from exc

    yield from project_series(
        dataset_id=dataset_id,
        series_dim_values=extract_series_dim_values(series_keys),
        dsd_order=dsd_order,
        labels=labels,
        augment=augment,
        augment_fragments=augment_fragments,
    )


def fetch_dataflow_with_structure(client: Any, dataset_id: str) -> Any:
    """Fetch dataflow with DSD + codelists. Tries descendants first,
    falls back to plain dataflow call if the agency rejects params."""
    try:
        return client.dataflow(
            resource_id=dataset_id,
            params={"references": "descendants"},
            force=True,
        )
    except TypeError:
        # Older sdmx1 signature or unsupported params — retry minimal.
        try:
            return client.dataflow(resource_id=dataset_id, force=True)
        except Exception as exc:
            raise SdmxFetchError(
                f"Failed to fetch dataflow {dataset_id}: {exc}"
            ) from exc
    except Exception as exc:
        raise SdmxFetchError(
            f"Failed to fetch dataflow {dataset_id}: {exc}"
        ) from exc


def resolve_dsd(client: Any, msg: Any, dataflow: Any, dataset_id: str) -> Any:
    """Return the DSD for ``dataflow`` — from ``msg`` if present, else fetch."""
    structure = getattr(dataflow, "structure", None)
    structure_id = getattr(structure, "id", None) if structure is not None else None
    if not structure_id:
        raise SdmxFetchError(
            f"Dataflow {dataset_id!r} has no DSD reference"
        )

    structures = getattr(msg, "structure", {}) or {}
    if structure_id in structures:
        return structures[structure_id]

    try:
        struct_msg = client.datastructure(resource_id=structure_id, force=True)
    except Exception as exc:
        raise SdmxFetchError(
            f"Failed to fetch DSD {structure_id} for {dataset_id}: {exc}"
        ) from exc
    try:
        return struct_msg.structure[structure_id]
    except (KeyError, AttributeError, TypeError) as exc:
        raise SdmxFetchError(
            f"DSD {structure_id} missing from follow-up fetch for {dataset_id}"
        ) from exc
