"""``sdmx_dimension_search`` — search or enumerate one dimension's values in a flow.

A flow's series catalog already indexes every dimension's labels (a per-dimension
BM25+Vector hybrid) and links each label to its code, so resolving "what values does this
dimension take?" is a read over the same cached catalog — no provider call.
"""

from __future__ import annotations

from typing import Annotated

import pandas as pd
from parsimony.catalog.storage import read_meta
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.result import Column, ColumnRole, OutputConfig
from pydantic import BaseModel, Field

from parsimony_sdmx.catalog_series import collect_distinct_from_columnar
from parsimony_sdmx.connectors.series_search import (
    ENUMERATION_LIMIT,
    _load_series_catalog,
    _not_published,
    _parse_agency,
    _resolve_catalog_path,
    _sdmx_meta,
)
from parsimony_sdmx.core.namespaces import series_namespace
from parsimony_sdmx.series_fields import SERIES_PARQUET, dim_label_field

#: A ``query`` ranks values into context (keep it small); omitting it enumerates every
#: populated value of the dimension into a variable (up to ``ENUMERATION_LIMIT``).
RANKED_LIMIT = 50


DIMENSION_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY),
        Column(name="label", role=ColumnRole.TITLE),
    ]
)


class DimensionSearchParams(BaseModel):
    agency: Annotated[str, Field(min_length=1, max_length=32)]
    dataset_id: Annotated[str, Field(min_length=1, max_length=128)]
    dimension: Annotated[str, Field(min_length=1, max_length=128)]
    query: str | None = Field(default=None, max_length=512)
    limit: int = Field(default=50, ge=1, le=ENUMERATION_LIMIT)
    catalog_root: str | None = None


@connector(output=DIMENSION_SEARCH_OUTPUT, tags=["sdmx", "tool"])
def sdmx_dimension_search(
    agency: str,
    dataset_id: str,
    dimension: str,
    query: str | None = None,
    limit: int = 50,
    catalog_root: str | None = None,
) -> pd.DataFrame:
    """Search or enumerate one dimension's values within a published SDMX flow.

    Given a flow (``agency`` + ``dataset_id``) and a ``dimension`` id (from the flow's
    ``dsd``), returns that dimension's ``(code, label)`` values. Pass ``query`` to rank the
    values by concept (e.g. "business" -> FREQ ``B`` "Business daily") as a shortlist
    (``limit`` <= 50); omit ``query`` to enumerate every value the flow populates (``limit``
    up to 10000). Use it to pick codes for a ``sdmx_series_search`` ``filter_json``, or to
    check whether a flow offers a value at all.
    """
    params = DimensionSearchParams(
        agency=agency,
        dataset_id=dataset_id,
        dimension=dimension,
        query=query,
        limit=limit,
        catalog_root=catalog_root,
    )
    q = (params.query or "").strip() or None
    if q is not None and params.limit > RANKED_LIMIT:
        raise InvalidParameterError(
            "sdmx",
            f"query= is a ranked shortlist (limit <= {RANKED_LIMIT}). To read every value, "
            f"omit query= and enumerate (limit up to {ENUMERATION_LIMIT}).",
        )

    agency_id = _parse_agency(params.agency)
    flow = params.dataset_id.strip()
    namespace = series_namespace(agency_id, flow)
    label = f"{agency_id.value}/{flow}"
    catalog_path = _resolve_catalog_path(namespace, label=label, catalog_root=params.catalog_root)
    if not catalog_path.is_dir():
        raise ConnectorError(_not_published(label), provider="sdmx")

    dsd_order = tuple(_sdmx_meta(catalog_path).get("dsd_order") or ())
    if params.dimension not in dsd_order:
        raise InvalidParameterError(
            "sdmx",
            f"unknown dimension {params.dimension!r} for {agency_id.value}/{flow}; valid dimensions: {list(dsd_order)}",
        )

    # Load the catalog defensively, mirroring sdmx_series_search: a corrupt snapshot raises a
    # bare ValueError (the framework's sha256 integrity check) that must surface as a typed
    # ConnectorError, not leak raw — both sibling connectors present the same failure identically.
    try:
        if q is not None:
            catalog = _load_series_catalog(namespace, str(catalog_path.resolve()))
            matches = catalog.search_values(q, field=dim_label_field(params.dimension), limit=params.limit)
            rows = [{"code": m.linked_value or m.value, "label": m.value} for m in matches]
        else:
            meta = read_meta(catalog_path)
            parquet_path = catalog_path / (meta.backend.rows_filename or SERIES_PARQUET)
            distinct = collect_distinct_from_columnar(parquet_path, (params.dimension,))[params.dimension]
            rows = [{"code": code, "label": label} for code, label in list(distinct.items())[: params.limit]]
    except (FileNotFoundError, ValueError) as exc:
        raise ConnectorError(f"Invalid series catalog for {namespace}: {exc}", provider="sdmx") from exc

    if not rows:
        raise EmptyDataError(
            "sdmx",
            f"No values for dimension {params.dimension!r} in {agency_id.value}/{flow} (query={params.query!r}).",
        )
    return pd.DataFrame(rows)


__all__ = ["DIMENSION_SEARCH_OUTPUT", "DimensionSearchParams", "sdmx_dimension_search"]
