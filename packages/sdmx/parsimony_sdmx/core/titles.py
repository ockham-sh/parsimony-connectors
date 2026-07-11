"""Deterministic title composition from SDMX codelist labels."""

from collections.abc import Mapping, Sequence

DIM_SEP = " - "


def compose_series_title(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
) -> str:
    """Concatenate per-dimension labels in DSD order.

    For each dimension in ``dsd_order``:
      * emit the codelist label when available
      * fall back to the raw code when no label is known
      * if the series has no value for this dimension, skip it

    Joined by ``" - "``.
    """
    return _compose_label_title(dim_values, dsd_order, labels)


def compose_observation_title(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
) -> str:
    """Concatenate per-dimension labels in DSD order (no codes in title).

    Sibling to :func:`compose_series_title`. The observation-fetch result
    exposes each dimension as a bare ``{dim}_code`` column, so this title is
    the row's human-readable surface — it carries the dimension labels that
    the code columns deliberately omit.

    For each dimension in ``dsd_order``:
      * emit the label if present
      * fall back to the raw code if no label is known
      * skip the dimension if the series has no value for it

    Joined by ``" - "``.
    """
    return _compose_label_title(dim_values, dsd_order, labels)


def choose_series_title(
    fallback: str,
    title: str | None = None,
    title_compl: str | None = None,
) -> str:
    """Prefer a source-provided SDMX series title, with label-title fallback.

    ``title_compl`` is intentionally ignored in the display title. It is
    usually descriptive prose or provenance that should remain explicit
    metadata if we decide to expose it, not be spliced into the title.

    Empty strings are treated as absent.
    """
    if title and title.strip():
        return title.strip()
    return fallback


def _compose_label_title(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
) -> str:
    parts: list[str] = []
    for dim_id in dsd_order:
        code = dim_values.get(dim_id)
        if not code:
            continue
        label = labels.get(dim_id, {}).get(code)
        parts.append(label or code)
    return DIM_SEP.join(parts)
