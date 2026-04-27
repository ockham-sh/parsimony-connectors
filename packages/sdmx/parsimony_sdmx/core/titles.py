"""Deterministic title composition from codelist labels."""

from collections.abc import Mapping, Sequence

DIM_SEP = " - "
CODE_LABEL_SEP = ": "
ECB_AUGMENT_SEP = " | "


def compose_series_title(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
) -> str:
    """Concatenate per-dimension ``"CODE: label"`` pairs in DSD order.

    For each dimension in ``dsd_order``:
      * if the series has a value and the codelist has a label,
        emit ``"CODE: label"``
      * if the series has a value but no label is known,
        emit the bare ``"CODE"`` (raw-code fallback)
      * if the series has no value for this dimension, skip it

    Joined by ``" - "``.
    """
    parts: list[str] = []
    for dim_id in dsd_order:
        code = dim_values.get(dim_id)
        if not code:
            continue
        label = labels.get(dim_id, {}).get(code)
        if label:
            parts.append(f"{code}{CODE_LABEL_SEP}{label}")
        else:
            parts.append(code)
    return DIM_SEP.join(parts)


def augment_with_ecb_attributes(
    base: str,
    title: str | None = None,
    title_compl: str | None = None,
) -> str:
    """Prefix ECB's natural-language TITLE to the codelist-composed base.

    Output shapes:

    * TITLE present:   ``"TITLE - base"`` (TITLE prepended to the dim
      composition)
    * TITLE absent:    ``"base"``

    The codelist ``base`` concatenates ``"CODE: label - CODE: label - …"``
    across every dimension — the authoritative record of every indexed
    property of the series. Earlier versions dropped it in favour of
    ECB's ``TITLE + TITLE_COMPL`` overlay to save embedder tokens, but
    ``TITLE_COMPL`` doesn't transcribe every dimension label (notably
    ``FREQ``, which stays in code form as ``"M"`` / ``"A"``). Only 24%
    of monthly HICP series carried the word "Monthly" in their indexed
    text, so queries like "Germany monthly HICP food annual rate"
    couldn't match on the frequency dimension. Keeping the base back
    restores that signal.

    ``title_compl`` is intentionally discarded. Its content is either
    covered by dim labels (e.g. "Neither seasonally nor working day
    adjusted" is the ADJUSTMENT=N label) or provenance prose
    ("Statistical Office of the European Commission (Eurostat)") that
    inflates token count without helping retrieval; ``dataset_id`` and
    ``agency`` metadata carry the source attribution.

    Empty strings are treated as absent.
    """
    if title:
        return f"{title}{DIM_SEP}{base}"
    return base
