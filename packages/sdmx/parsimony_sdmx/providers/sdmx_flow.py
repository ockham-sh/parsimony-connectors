"""Shared SDMX flow for agencies whose discovery uses ``sdmx1.series_keys``.

ESTAT, IMF_DATA, and ECB all follow the same two-call shape:

1. ``client.dataflow(resource_id=DATASET, params={references: descendants})``
   returns a structure message with the DSD and all codelists needed for
   the dataset.
2. ``client.series_keys(DATASET)`` returns a ``SeriesKey`` stream.

ECB additionally exposes per-series TITLE / TITLE_COMPL via an XML
endpoint; TITLE is passed here as the optional source-title hook.

WB diverges entirely and lives in its own module (T9).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from typing import Any

from parsimony_sdmx.core.codelists import resolve_codelists
from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import (
    CodelistCode,
    CodelistRecord,
    DatasetRecord,
    DimensionStructure,
    SeriesRecord,
    StructureRecord,
)
from parsimony_sdmx.core.projection import (
    SeriesTitleProvider,
    project_series,
)
from parsimony_sdmx.providers.sdmx_extract import (
    extract_dimension_codelist_ids,
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
        raise SdmxFetchError(f"Failed to list dataflows for {agency_id}: {exc}") from exc

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


def structure_from_message(
    msg: Any,
    client: Any,
    *,
    agency_id: str,
    dataset_id: str,
    language_prefs: Sequence[str] = ("en",),
    max_sample_codes: int = 5,
) -> StructureRecord:
    """Build a :class:`StructureRecord` from an already-fetched structure message."""
    try:
        dataflow = msg.dataflow[dataset_id]
    except (KeyError, AttributeError, TypeError) as exc:
        raise SdmxFetchError(f"Dataflow {dataset_id!r} missing from response for {agency_id}") from exc

    title = extract_flow_title(dataflow, language_prefs)
    dsd = resolve_dsd(client, msg, dataflow, dataset_id)
    dsd_order = tuple(extract_dsd_dim_order(dsd, exclude_time=True))
    dim_cl_ids = extract_dimension_codelist_ids(dsd, exclude_time=True)
    raw_codelists = extract_raw_codelists(dsd, msg)
    labels = resolve_codelists(raw_codelists, language_prefs)

    dimensions: list[DimensionStructure] = []
    codelists_by_id: dict[str, list[CodelistCode]] = {}

    for dim_id in dsd_order:
        cl_id = dim_cl_ids.get(dim_id)
        dim_labels = labels.get(dim_id, {})
        codes = sorted(dim_labels.items())
        sample = tuple(CodelistCode(code=c, label=lab) for c, lab in codes[:max_sample_codes])
        dimensions.append(
            DimensionStructure(
                dimension_id=dim_id,
                codelist_id=cl_id,
                name=dim_id,
                code_count=len(codes),
                sample=sample,
            )
        )
        if cl_id and dim_labels:
            bucket = codelists_by_id.setdefault(cl_id, [])
            seen = {item.code for item in bucket}
            for code, label in codes:
                if code not in seen:
                    bucket.append(CodelistCode(code=code, label=label))
                    seen.add(code)

    codelist_records = tuple(
        CodelistRecord(codelist_id=cl_id, codes=tuple(entries)) for cl_id, entries in sorted(codelists_by_id.items())
    )

    return StructureRecord(
        dataset_id=dataset_id,
        agency_id=agency_id,
        title=title,
        dsd_order=dsd_order,
        dimensions=tuple(dimensions),
        codelists=codelist_records,
    )


def list_structure_flow(
    client: Any,
    agency_id: str,
    dataset_id: str,
    language_prefs: Sequence[str] = ("en",),
    *,
    max_sample_codes: int = 5,
) -> StructureRecord:
    """Fetch DSD + codelists for one dataflow — no ``series_keys`` call."""
    msg = fetch_dataflow_with_structure(client, dataset_id)
    return structure_from_message(
        msg,
        client,
        agency_id=agency_id,
        dataset_id=dataset_id,
        language_prefs=language_prefs,
        max_sample_codes=max_sample_codes,
    )


def list_series_flow(
    client: Any,
    agency_id: str,
    dataset_id: str,
    language_prefs: Sequence[str] = ("en",),
    source_title: SeriesTitleProvider | None = None,
) -> Iterator[SeriesRecord]:
    """Fetch DSD + codelists + series keys, yield ``SeriesRecord`` per series."""
    msg = fetch_dataflow_with_structure(client, dataset_id)

    try:
        dataflow = msg.dataflow[dataset_id]
    except (KeyError, AttributeError, TypeError) as exc:
        raise SdmxFetchError(f"Dataflow {dataset_id!r} missing from response for {agency_id}") from exc

    dsd = resolve_dsd(client, msg, dataflow, dataset_id)
    dsd_order = extract_dsd_dim_order(dsd, exclude_time=True)
    raw_codelists = extract_raw_codelists(dsd, msg)
    labels = resolve_codelists(raw_codelists, language_prefs)

    try:
        series_keys = client.series_keys(dataset_id)
    except Exception as exc:
        raise SdmxFetchError(f"Failed to fetch series keys for {dataset_id}: {exc}") from exc

    yield from project_series(
        dataset_id=dataset_id,
        series_dim_values=extract_series_dim_values(series_keys),
        dsd_order=dsd_order,
        labels=labels,
        source_title=source_title,
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
            raise SdmxFetchError(f"Failed to fetch dataflow {dataset_id}: {exc}") from exc
    except Exception as exc:
        raise SdmxFetchError(f"Failed to fetch dataflow {dataset_id}: {exc}") from exc


def resolve_dsd(client: Any, msg: Any, dataflow: Any, dataset_id: str) -> Any:
    """Return the DSD for ``dataflow`` — from ``msg`` if present, else fetch."""
    structure = getattr(dataflow, "structure", None)
    structure_id = getattr(structure, "id", None) if structure is not None else None
    if not structure_id:
        raise SdmxFetchError(f"Dataflow {dataset_id!r} has no DSD reference")

    structures = getattr(msg, "structure", {}) or {}
    if structure_id in structures:
        return structures[structure_id]

    try:
        struct_msg = client.datastructure(resource_id=structure_id, force=True)
    except Exception as exc:
        raise SdmxFetchError(f"Failed to fetch DSD {structure_id} for {dataset_id}: {exc}") from exc
    try:
        return struct_msg.structure[structure_id]
    except (KeyError, AttributeError, TypeError) as exc:
        raise SdmxFetchError(f"DSD {structure_id} missing from follow-up fetch for {dataset_id}") from exc
