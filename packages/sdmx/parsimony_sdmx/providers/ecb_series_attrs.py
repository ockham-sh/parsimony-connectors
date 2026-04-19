"""Parse ECB ``?detail=nodata`` XML into ``{series_id: (TITLE, TITLE_COMPL)}``.

The ECB endpoint returns every series for a dataflow with attributes
but no observations. Responses are routinely hundreds of MB. We use
:func:`parsimony_sdmx.io.xml.iter_elements` so elements are cleared after
being read, keeping peak memory bounded.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from parsimony_sdmx.io.xml import iter_elements

GENERIC_NS = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
SERIES_TAG = f"{{{GENERIC_NS}}}Series"
SERIES_KEY_TAG = f"{{{GENERIC_NS}}}SeriesKey"
ATTRIBUTES_TAG = f"{{{GENERIC_NS}}}Attributes"
VALUE_TAG = f"{{{GENERIC_NS}}}Value"

TITLE_ATTR = "TITLE"
TITLE_COMPL_ATTR = "TITLE_COMPL"


def parse_ecb_series_attributes(
    xml_bytes: bytes,
    dim_order: Sequence[str],
) -> dict[str, tuple[str | None, str | None]]:
    """Return ``{series_id: (TITLE, TITLE_COMPL)}`` for every series in the response.

    ``series_id`` is the concatenation of dim values in ``dim_order``
    joined by ``"."``. Series whose key is missing a dimension listed in
    ``dim_order`` are skipped.
    """
    result: dict[str, tuple[str | None, str | None]] = {}
    for series_elem in iter_elements(xml_bytes, SERIES_TAG):
        # MUST read inside the loop — iter_elements clears after yield.
        key_values, attr_values = _extract_key_and_attrs(series_elem)
        series_id = _build_series_id(key_values, dim_order)
        if series_id is None:
            continue
        result[series_id] = (
            attr_values.get(TITLE_ATTR),
            attr_values.get(TITLE_COMPL_ATTR),
        )
    return result


def _extract_key_and_attrs(
    series_elem: Any,
) -> tuple[dict[str, str], dict[str, str]]:
    key_elem = series_elem.find(SERIES_KEY_TAG)
    attrs_elem = series_elem.find(ATTRIBUTES_TAG)
    key_values = _extract_values(key_elem)
    attr_values = _extract_values(attrs_elem)
    return key_values, attr_values


def _extract_values(elem: Any) -> dict[str, str]:
    if elem is None:
        return {}
    out: dict[str, str] = {}
    for v in elem.findall(VALUE_TAG):
        k = v.get("id")
        val = v.get("value")
        if k and val is not None:
            out[k] = val
    return out


def _build_series_id(
    key_values: dict[str, str], dim_order: Sequence[str]
) -> str | None:
    parts: list[str] = []
    for dim in dim_order:
        v = key_values.get(dim)
        if v is None:
            return None
        parts.append(v)
    return ".".join(parts)
