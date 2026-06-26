"""Pure (network-free) helpers that turn raw Webstat rows into catalog rows.

Kept separate from the enumerator so the row-shaping logic is unit-testable
without any HTTP. Two builders — one per entity kind — plus the dedup/assembly
entry point :func:`build_enumerate_rows`.
"""

from __future__ import annotations

from typing import Any

from parsimony_bdf.outputs import ENUMERATE_COLUMNS

# A blank row with every declared column present — @enumerator requires an
# EXACT column match, so every emitted row must carry all of them.
_BLANK: dict[str, str] = {name: "" for name in ENUMERATE_COLUMNS}


def _clean(value: Any) -> str:
    """Coerce a raw field to a stripped string (``None`` → empty)."""
    if value is None:
        return ""
    return str(value).strip()


def _as_path(raw: Any) -> str:
    """Flatten a Webstat ``path_*`` field into a single breadcrumb string.

    The export returns ``path_en`` as a JSON array (e.g.
    ``['Rates and prices/Market interest rates']``), occasionally a bare string.
    Returns the unique non-empty segments joined by `` | ``.
    """
    if raw is None:
        return ""
    items = raw if isinstance(raw, (list, tuple)) else [raw]
    seen: list[str] = []
    for item in items:
        text = _clean(item)
        if text and text not in seen:
            seen.append(text)
    return " | ".join(seen)


def _first_nonempty(*candidates: str) -> str:
    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def _dataset_description(*, name_en: str, name_fr: str, desc_en: str, desc_fr: str) -> str:
    """Bilingual description for a dataset stub (deduping identical halves)."""
    parts: list[str] = []
    for value in (name_en, name_fr, desc_en, desc_fr):
        if value and value.lower() not in {p.lower() for p in parts}:
            parts.append(value)
    return " | ".join(parts)


def dataset_stub_row(dataset: dict[str, Any]) -> dict[str, str] | None:
    """Build the synthetic ``dataset:{id}`` parent row, or ``None`` if unusable."""
    dataset_id = _clean(dataset.get("dataset_id"))
    if not dataset_id:
        return None
    name_en = _clean(dataset.get("name_en"))
    name_fr = _clean(dataset.get("name_fr"))
    desc_en = _clean(dataset.get("description_en"))
    desc_fr = _clean(dataset.get("description_fr"))
    return {
        **_BLANK,
        "code": f"dataset:{dataset_id}",
        "title": _first_nonempty(name_en, name_fr, dataset_id),
        "description": _dataset_description(name_en=name_en, name_fr=name_fr, desc_en=desc_en, desc_fr=desc_fr),
        "entity_type": "dataset",
        "dataset_id": dataset_id,
    }


def _series_description(
    *,
    title_en: str,
    title_fr: str,
    long_en: str,
    long_fr: str,
    path: str,
    dataset_id: str,
    dataset_name: str,
    source_agency: str,
) -> str:
    """Fold bilingual titles, breadcrumb and dataset context into one string.

    Both languages plus the hierarchy path land in ``description`` (which the
    discovery index covers), so an agent searching in English or French — or by
    topic breadcrumb — gets a lexical hit even though ``title`` is single-language.
    """
    bilingual_en = _first_nonempty(long_en, title_en)
    bilingual_fr = _first_nonempty(long_fr, title_fr)
    parts: list[str] = []
    if bilingual_en:
        parts.append(bilingual_en)
    if bilingual_fr and bilingual_fr.lower() != bilingual_en.lower():
        parts.append(bilingual_fr)
    if path:
        parts.append(path)
    ds_ctx = dataset_name or dataset_id
    if ds_ctx:
        parts.append(f"Dataset: {ds_ctx}.")
    if source_agency:
        parts.append(f"Source: {source_agency}.")
    return " | ".join(p for p in parts if p)


def series_row(series: dict[str, Any], dataset_names: dict[str, str]) -> dict[str, str] | None:
    """Build a catalog row for one series, or ``None`` if it has no usable key."""
    series_key = _clean(series.get("series_key"))
    if not series_key:
        return None
    dataset_id = _clean(series.get("dataset_id"))
    title_en = _clean(series.get("title_en"))
    title_fr = _clean(series.get("title_fr"))
    long_en = _clean(series.get("title_long_en"))
    long_fr = _clean(series.get("title_long_fr"))
    path = _as_path(series.get("path_en")) or _as_path(series.get("path_fr"))
    source_agency = _clean(series.get("source_agency"))
    return {
        **_BLANK,
        "code": series_key,
        "title": _first_nonempty(title_en, title_fr, long_en, long_fr, series_key),
        "description": _series_description(
            title_en=title_en,
            title_fr=title_fr,
            long_en=long_en,
            long_fr=long_fr,
            path=path,
            dataset_id=dataset_id,
            dataset_name=dataset_names.get(dataset_id, ""),
            source_agency=source_agency,
        ),
        "entity_type": "series",
        "dataset_id": dataset_id,
        "frequency": _clean(series.get("freq")),
        "ref_area": _clean(series.get("ref_area")),
        "source_agency": source_agency,
        "path": path,
        "first_time_period": _clean(series.get("first_time_period_date")),
        "last_time_period": _clean(series.get("last_time_period_date")),
    }


def build_enumerate_rows(
    datasets: list[dict[str, Any]],
    series: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Assemble catalog rows: dataset stubs first, then series, deduped by code.

    Dataset stubs come first so that on the (defensive) chance a series key
    collides with a stub code, the stub wins. Series keys are globally unique in
    practice, so the dedup mainly guards against accidental repeats in the feed.
    """
    dataset_names = {
        _clean(d.get("dataset_id")): _first_nonempty(_clean(d.get("name_en")), _clean(d.get("name_fr")))
        for d in datasets
        if _clean(d.get("dataset_id"))
    }

    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for dataset in datasets:
        row = dataset_stub_row(dataset)
        if row is not None and row["code"] not in seen:
            seen.add(row["code"])
            rows.append(row)

    for item in series:
        row = series_row(item, dataset_names)
        if row is not None and row["code"] not in seen:
            seen.add(row["code"])
            rows.append(row)

    return rows


__all__ = ["build_enumerate_rows", "dataset_stub_row", "series_row"]
