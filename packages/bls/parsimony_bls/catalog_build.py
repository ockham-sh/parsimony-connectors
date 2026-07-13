"""Build the two-tier BLS catalogs.

Tier 1 (``bls_surveys``): one entity per survey, always built, complete. A
``dimensions`` manifest is attached for surveys whose series catalog has been
built. Tier 2 (``bls_series_<survey>``): one entity per series in a survey, built
for the headline surveys and lazy-buildable on demand for any indexable survey.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from parsimony.catalog import Catalog, Entity
from parsimony.result import Result

from parsimony_bls.catalog_policy import (
    discover_dim_codes,
    manifest_from_series_entries,
    series_entries,
    series_indexes,
    surveys_indexes,
)
from parsimony_bls.connectors.enumerate_series import enumerate_bls_series
from parsimony_bls.connectors.enumerate_surveys import enumerate_bls_surveys
from parsimony_bls.outputs import series_enum_output
from parsimony_bls.surveys import SURVEYS_NAMESPACE, normalize_survey, series_namespace


def build_series_catalog(survey: str, *, max_rows: int = 0) -> Catalog:
    """Build one per-survey series catalog from the live ``.series`` enumeration."""
    sv = normalize_survey(survey)
    result = enumerate_bls_series(survey=sv, max_rows=max_rows)
    raw_entries = list(Result(raw=result.raw, output_spec=series_enum_output(sv)).entities.values())
    dim_codes = discover_dim_codes(raw_entries)
    entries = series_entries(raw_entries, dim_codes)
    catalog = Catalog(series_namespace(sv), default_field="title")
    catalog.set_entities(entries)
    catalog.set_indexes(series_indexes(entries, dim_codes))
    catalog.build()
    return catalog


def attach_manifests(entries: Sequence[Entity], manifests: dict[str, list[dict[str, Any]]]) -> list[Entity]:
    """Attach a ``dimensions`` manifest to survey entries that have one."""
    out: list[Entity] = []
    for entry in entries:
        survey = str(entry.metadata.get("survey", entry.code)).upper()
        manifest = manifests.get(survey)
        if manifest is None:
            out.append(entry)
            continue
        metadata = dict(entry.metadata)
        metadata["dimensions"] = manifest
        out.append(
            Entity(
                namespace=entry.namespace,
                code=entry.code,
                title=entry.title,
                metadata=metadata,
            )
        )
    return out


def build_surveys_catalog(
    *,
    api_key: str = "",
    manifests: dict[str, list[dict[str, Any]]] | None = None,
) -> Catalog:
    """Build the tier-1 surveys catalog, optionally attaching dimension manifests."""
    result = enumerate_bls_surveys(api_key=api_key)
    entries = list(result.entities.values())
    if manifests:
        entries = attach_manifests(entries, manifests)
    catalog = Catalog(SURVEYS_NAMESPACE, indexes=surveys_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


def manifest_for_survey(survey: str, *, max_rows: int = 0) -> list[dict[str, Any]]:
    """Build a survey's dimension manifest from its series entries (for tier 1)."""
    sv = normalize_survey(survey)
    result = enumerate_bls_series(survey=sv, max_rows=max_rows)
    raw_entries = list(Result(raw=result.raw, output_spec=series_enum_output(sv)).entities.values())
    return manifest_from_series_entries(raw_entries)


__all__ = [
    "attach_manifests",
    "build_series_catalog",
    "build_surveys_catalog",
    "manifest_for_survey",
]
