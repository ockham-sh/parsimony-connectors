"""Translate ``sdmx1`` objects into the plain-data shapes ``core`` consumes.

Providers call these helpers so the pure transforms in
:mod:`parsimony_sdmx.core` never import from ``sdmx1``. Every function here
is a thin projection — no network, no I/O — and is testable by
constructing small fake objects with ``types.SimpleNamespace``.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from typing import Any

from bs4 import BeautifulSoup

from parsimony_sdmx.core.codelists import pick_label

TIME_DIM = "TIME_PERIOD"

_WHITESPACE_RE = re.compile(r"\s+")


def clean_text(text: str) -> str:
    """Strip HTML tags and normalise whitespace.

    ESTAT dataflow descriptions embed ``<p>``, ``<br>``, and named
    character entities verbatim from the source CMS. We emit plain
    prose so downstream title strings are human-readable and free of
    render-specific markup.
    """
    if not text:
        return text
    if "<" in text or "&" in text:
        text = BeautifulSoup(text, "lxml").get_text(separator=" ")
    return _WHITESPACE_RE.sub(" ", text).strip()


def extract_flow_title(flow: Any, language_prefs: Sequence[str] = ("en",)) -> str:
    """Build ``"{name} - {description}"`` for a dataflow, or just ``name``.

    Falls back to the dataflow ``id`` if localized name is empty.
    HTML markup and whitespace in name/description are normalised.
    """
    name = _pick_localized(getattr(flow, "name", None), language_prefs)
    name = clean_text(name) if name else ""
    if not name:
        name = str(getattr(flow, "id", "") or "")
    description = _pick_localized(getattr(flow, "description", None), language_prefs)
    description = clean_text(description) if description else ""
    if description:
        return f"{name} - {description}"
    return name


def extract_dsd_dim_order(dsd: Any, exclude_time: bool = True) -> list[str]:
    """Return dimension IDs in DSD order, optionally excluding ``TIME_PERIOD``."""
    result: list[str] = []
    for dim in dsd.dimensions:
        dim_id = getattr(dim, "id", None)
        if not dim_id:
            continue
        if exclude_time and dim_id == TIME_DIM:
            continue
        result.append(dim_id)
    return result


def extract_raw_codelists(
    dsd: Any,
    msg: Any,
) -> dict[str, dict[str, dict[str, str]]]:
    """Build ``{dim_id: {code_id: {lang: label}}}`` from DSD + structure message."""
    raw: dict[str, dict[str, dict[str, str]]] = {}
    codelists = getattr(msg, "codelist", {}) or {}
    for dim in dsd.dimensions:
        dim_id = getattr(dim, "id", None)
        if not dim_id or dim_id == TIME_DIM:
            continue
        cl_id = _find_codelist_id(dim)
        if cl_id is None:
            continue
        codelist = codelists.get(cl_id)
        if codelist is None:
            continue
        dim_map: dict[str, dict[str, str]] = {}
        for code in codelist:
            code_id = getattr(code, "id", None)
            if not code_id:
                continue
            dim_map[code_id] = _localizations(getattr(code, "name", None))
        raw[dim_id] = dim_map
    return raw


def extract_series_dim_values(series_keys: Any) -> Iterator[dict[str, str]]:
    """Yield one ``{dim_id: code}`` dict per ``SeriesKey``.

    Accepts either a dict-like (keyed by series id) or a plain iterable
    of ``SeriesKey`` objects — both shapes are returned by different
    ``sdmx1`` call paths.
    """
    items = series_keys.values() if hasattr(series_keys, "values") else series_keys
    for sk in items:
        yield _series_key_to_dict(sk)


def _series_key_to_dict(sk: Any) -> dict[str, str]:
    """Convert one ``SeriesKey`` to ``{dim_id: code}``.

    ``SeriesKey.values`` is a dict ``{dim_id: KeyValue}`` where
    ``KeyValue.value`` carries the code string.
    """
    result: dict[str, str] = {}
    values = getattr(sk, "values", None)
    if values is None:
        return result
    items = values.items() if hasattr(values, "items") else values
    for item in items:
        if isinstance(item, tuple) and len(item) == 2:
            dim_id, kv = item
        else:
            kv = item
            dim_id = getattr(kv, "id", None)
        code = getattr(kv, "value", None)
        if dim_id is None or code is None:
            continue
        result[str(dim_id)] = str(code)
    return result


def _find_codelist_id(dim: Any) -> str | None:
    local = getattr(dim, "local_representation", None)
    enumerated = getattr(local, "enumerated", None) if local is not None else None
    if enumerated is not None:
        return str(getattr(enumerated, "id", "")) or None
    concept = getattr(dim, "concept_identity", None)
    core = getattr(concept, "core_representation", None) if concept is not None else None
    enumerated = getattr(core, "enumerated", None) if core is not None else None
    if enumerated is not None:
        return str(getattr(enumerated, "id", "")) or None
    return None


def _localizations(named: Any) -> dict[str, str]:
    if named is None:
        return {}
    locs = getattr(named, "localizations", None) or {}
    return {str(k): str(v) for k, v in locs.items()}


def _pick_localized(named: Any, prefs: Sequence[str]) -> str | None:
    return pick_label(_localizations(named), prefs)
