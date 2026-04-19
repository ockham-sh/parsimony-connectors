"""Language-aware codelist resolution.

The title builder needs one label per (dim, code). Codelists arrive with
localizations per language. This module picks the best available label
using a caller-supplied language preference list, falling back to any
non-empty localization.
"""

from collections.abc import Mapping, Sequence


def pick_label(
    localizations: Mapping[str, str],
    language_prefs: Sequence[str],
) -> str | None:
    """Pick the first non-empty label matching language preferences.

    If no preferred language is present, return the first non-empty
    label in any available language. If no localizations carry a
    non-empty string, return None.
    """
    for lang in language_prefs:
        value = localizations.get(lang)
        if value:
            return value
    for value in localizations.values():
        if value:
            return value
    return None


def resolve_codelists(
    raw: Mapping[str, Mapping[str, Mapping[str, str]]],
    language_prefs: Sequence[str] = ("en",),
) -> dict[str, dict[str, str]]:
    """Resolve raw ``{dim_id: {code_id: {lang: label}}}`` to ``{dim_id: {code_id: label}}``.

    Codes whose localizations yield no usable label are omitted from the
    resolved map. The title builder treats a missing entry as "no label
    known for this code" and falls back to the raw code.
    """
    resolved: dict[str, dict[str, str]] = {}
    for dim_id, codes in raw.items():
        dim_labels: dict[str, str] = {}
        for code_id, localizations in codes.items():
            label = pick_label(localizations, language_prefs)
            if label is not None:
                dim_labels[code_id] = label
        resolved[dim_id] = dim_labels
    return resolved
