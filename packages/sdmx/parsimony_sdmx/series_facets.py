"""Facet helpers for SDMX series catalog search results."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.compute as pc

from parsimony_sdmx.series_fields import dim_code_field, dim_label_field

MAX_FACET_VALUES = 50


def facets_from_table(
    table: pa.Table,
    dsd_order: tuple[str, ...],
    *,
    pinned_dims: set[str],
    max_values: int = MAX_FACET_VALUES,
) -> dict[str, list[tuple[str, str, int]]]:
    """Distinct (code, label, count) per unpinned dimension on *table*."""
    facets: dict[str, list[tuple[str, str, int]]] = {}
    for dim in dsd_order:
        if dim in pinned_dims:
            continue
        code_col = dim_code_field(dim)
        label_col = dim_label_field(dim)
        if code_col not in table.column_names:
            continue
        counts = pc.value_counts(table[code_col])  # type: ignore[attr-defined]
        code_values = counts.field("values").to_pylist()
        code_counts = counts.field("counts").to_pylist()
        label_lookup: dict[str, str] = {}
        if label_col in table.column_names:
            pairs = table.select([code_col, label_col]).to_pylist()
            for pair in pairs:
                code = str(pair.get(code_col, ""))
                if code and code not in label_lookup:
                    label_lookup[code] = str(pair.get(label_col, code))

        entries: list[tuple[str, str, int]] = []
        for code, count in zip(code_values, code_counts, strict=True):
            code_s = str(code)
            label_s = label_lookup.get(code_s, code_s)
            entries.append((code_s, label_s, int(count)))
        entries.sort(key=lambda item: (-item[2], item[0]))
        facets[dim] = entries[:max_values]
    return facets


def facets_to_json(facets: dict[str, list[tuple[str, str, int]]]) -> str:
    """Serialize facets for the connector ``refine`` column."""
    payload = {
        dim: [{"code": code, "label": label, "count": count} for code, label, count in values]
        for dim, values in facets.items()
    }
    return json.dumps(payload, separators=(",", ":"))


__all__ = [
    "MAX_FACET_VALUES",
    "facets_from_table",
    "facets_to_json",
]
