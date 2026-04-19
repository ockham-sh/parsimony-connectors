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
    """Append ECB TITLE / TITLE_COMPL after the codelist base.

    Both present:   ``"base | TITLE - TITLE_COMPL"``
    Only one:       ``"base | that_one"``
    Neither:        ``"base"``

    Empty strings are treated as absent.
    """
    pieces = [p for p in (title, title_compl) if p]
    if not pieces:
        return base
    return f"{base}{ECB_AUGMENT_SEP}{DIM_SEP.join(pieces)}"
