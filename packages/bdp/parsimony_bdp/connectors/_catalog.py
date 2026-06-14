"""Pure, network-free row builders for the BdP catalog.

The enumerator crawls the BPstat hierarchy and feeds raw domain / dataset /
series stubs through these helpers to emit ``BDP_ENUMERATE_OUTPUT``-shaped rows.
The rich bilingual descriptions are layered on separately at build time via
:func:`apply_enrichment` (the crawl only yields a terse English ``label``; the
``/series/`` endpoint carries the search-bearing prose). Keeping all of this
network-free makes it unit-testable without mocking HTTP.
"""

from __future__ import annotations

from typing import Any, cast

import pandas as pd
from parsimony_shared.cb_enumerate import truncate_description

from parsimony_bdp.outputs import ENUMERATE_COLUMNS


def clean(value: Any) -> str:
    """Coerce to a stripped string (``None`` → empty)."""
    return str(value).strip() if value is not None else ""


def _blank_row() -> dict[str, str]:
    return dict.fromkeys(ENUMERATE_COLUMNS, "")


def series_code(domain_id: str, dataset_id: str, series_id: str) -> str:
    """The compound catalog code for a series (globally unique, fetch-dispatchable)."""
    return f"{domain_id}:{dataset_id}:{series_id}"


def _series_description(*, label: str, dataset_label: str, domain_name: str) -> str:
    """Assemble the baseline (crawl-only) DESCRIPTION for a series.

    Folds the English label and the dataset / domain context. The rich
    bilingual ``/series/`` descriptions are appended later by
    :func:`apply_enrichment`; this is the fallback that stands alone when the
    enumerator is used without a build-time enrichment pass.
    """
    chunks: list[str] = []
    if label:
        chunks.append(f"{label}.")
    if dataset_label:
        chunks.append(f"{dataset_label}.")
    if domain_name:
        chunks.append(f"Domain: {domain_name}.")
    chunks.append("Banco de Portugal BPstat.")
    return cast(str, truncate_description(" ".join(c for c in chunks if c).strip()))


def domain_row(
    *,
    domain_id: str,
    name: str,
    description: str,
    num_series: int,
    num_datasets: int,
    last_update: str,
) -> dict[str, str]:
    """Synthetic ``domain:<id>`` navigation row."""
    boilerplate = "Banco de Portugal economic statistics."
    body = f"{boilerplate} {description}" if description else boilerplate
    parts = [
        f"BPstat domain: {name}." if name else "",
        body,
        f"Holds {num_datasets} datasets and {num_series} series." if (num_datasets or num_series) else "",
    ]
    row = _blank_row()
    row.update(
        code=f"domain:{domain_id}",
        title=name,
        description=cast(str, truncate_description(" ".join(p for p in parts if p).strip())),
        entity_type="domain",
        domain_id=domain_id,
        domain_name=name,
        num_series=str(num_series or ""),
        last_update=last_update,
        source="bpstat",
    )
    return row


def dataset_row(
    *,
    domain_id: str,
    domain_name: str,
    dataset_id: str,
    dataset_label: str,
    num_series: int,
    last_update: str,
) -> dict[str, str]:
    """Synthetic ``dataset:<domain>:<id>`` navigation row."""
    parts = [
        f"{dataset_label}." if dataset_label else "",
        f"Banco de Portugal dataset under domain '{domain_name}'.",
        f"Holds {num_series} series." if num_series else "",
    ]
    row = _blank_row()
    row.update(
        code=f"dataset:{domain_id}:{dataset_id}",
        title=dataset_label,
        description=cast(str, truncate_description(" ".join(p for p in parts if p).strip())),
        entity_type="dataset",
        domain_id=domain_id,
        domain_name=domain_name,
        dataset_id=dataset_id,
        dataset_label=dataset_label,
        num_series=str(num_series or ""),
        last_update=last_update,
        source="bpstat",
    )
    return row


def series_row(
    *,
    domain_id: str,
    domain_name: str,
    dataset_id: str,
    dataset_label: str,
    series_id: str,
    label: str,
    last_update: str,
) -> dict[str, str]:
    """One catalog row for a discovered series (pre-enrichment)."""
    row = _blank_row()
    row.update(
        code=series_code(domain_id, dataset_id, series_id),
        title=label or series_id,
        description=_series_description(label=label, dataset_label=dataset_label, domain_name=domain_name),
        entity_type="series",
        domain_id=domain_id,
        domain_name=domain_name,
        dataset_id=dataset_id,
        dataset_label=dataset_label,
        last_update=last_update,
        source="bpstat",
    )
    return row


def series_id_from_code(code: str) -> str | None:
    """Recover the bare ``series_id`` from a ``domain:dataset:series`` code.

    Returns ``None`` for synthetic ``domain:``/``dataset:`` stub codes.
    """
    if code.startswith(("domain:", "dataset:")):
        return None
    parts = code.split(":")
    return parts[-1] if len(parts) == 3 else None


def apply_enrichment(
    df: pd.DataFrame,
    *,
    enrich_en: dict[str, dict[str, str]],
    enrich_pt: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """Overlay rich bilingual ``/series/`` metadata onto the series rows.

    Returns a NEW frame (the input is not mutated). For each ``series`` row it
    fills ``short_label`` / ``title_pt`` from the EN / PT metadata and appends
    the rich EN + PT descriptions to the baseline ``description`` (so the BM25
    index sees both languages). Missing enrichment leaves a row on its crawl
    fallback. ``domain`` / ``dataset`` stub rows pass through untouched.
    """
    if df.empty:
        return df.copy()

    descriptions = df["description"].tolist()
    short_labels = df["short_label"].tolist()
    titles_pt = df["title_pt"].tolist()

    for i, code in enumerate(df["code"].tolist()):
        sid = series_id_from_code(str(code))
        if sid is None:
            continue
        en = enrich_en.get(sid, {})
        pt = enrich_pt.get(sid, {})
        if en.get("short_label"):
            short_labels[i] = en["short_label"]
        if pt.get("label"):
            titles_pt[i] = pt["label"]
        extra = [descriptions[i]]
        if en.get("description"):
            extra.append(en["description"])
        if pt.get("description"):
            extra.append(f"PT: {pt['description']}")
        descriptions[i] = cast(str, truncate_description(" ".join(x for x in extra if x).strip()))

    out = df.copy()
    out["description"] = descriptions
    out["short_label"] = short_labels
    out["title_pt"] = titles_pt
    return out
