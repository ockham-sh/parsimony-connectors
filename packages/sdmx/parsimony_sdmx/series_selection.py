"""Heuristics for which SDMX flows get per-dataset series catalogs.

Operator tooling — not part of the plugin contract. Small agencies and
central-bank sources typically get full series coverage; large statistical
offices get a finance/macro subset keyed off dataset id + title.
"""

from __future__ import annotations

import re

from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.core.models import DatasetRecord

# Agencies where essentially every listed flow is macro-relevant.
_BUILD_ALL_SERIES: frozenset[AgencyId] = frozenset({AgencyId.ECB, AgencyId.IMF_DATA, AgencyId.WB_WDI})

# Below this dataset count (non-derived flows), index series for all flows.
_SMALL_AGENCY_THRESHOLD = 80

_MACRO_TITLE_RE = re.compile(
    r"\b("
    r"gdp|inflation|cpi|hicp|price|employment|unemployment|wage|interest|yield|"
    r"exchange|fx|money|credit|loan|debt|fiscal|budget|trade|balance|payment|"
    r"current account|financial|monetary|bank|reserve|deposit|bond|security|"
    r"rate|index|production|industrial|retail|confidence|pmi|forecast|"
    r"national account|government|tax|revenue|expenditure|import|export|"
    r"labour|labor|housing|mortgage|equity|stock|market|commodity|energy|"
    r"macro|economic|finance|financial"
    r")\b",
    re.IGNORECASE,
)

_NON_MACRO_TITLE_RE = re.compile(
    r"\b("
    r"regional|municipal|nace|agriculture|forestry|fishery|fishing|livestock|"
    r"tourism|hotel|restaurant|culture|sport|environment|waste|water|air|"
    r"health care|hospital|education|school|research|patent|"
    r"transport equipment|vehicle registration|road accident|"
    r"energy balance|electricity generation by fuel"
    r")\b",
    re.IGNORECASE,
)

_NON_MACRO_ID_PREFIXES = (
    "REG_",
    "AGR_",
    "FISH",
    "TOUR",
    "ENV_",
    "EDU_",
    "HEALTH",
    "TRAN_",
)


def _is_derived(record: DatasetRecord) -> bool:
    return "$" in record.dataset_id


def _macro_relevant_estat(record: DatasetRecord) -> bool:
    blob = f"{record.dataset_id} {record.title}"
    if _NON_MACRO_TITLE_RE.search(blob):
        return False
    upper_id = record.dataset_id.upper()
    if any(upper_id.startswith(p) for p in _NON_MACRO_ID_PREFIXES):
        return False
    return _MACRO_TITLE_RE.search(blob) is not None


def should_build_series_catalog(
    agency: AgencyId,
    record: DatasetRecord,
    *,
    total_flows: int,
) -> bool:
    """Return whether *record* should receive a ``sdmx_series_*`` snapshot."""

    if _is_derived(record):
        return False
    if agency in _BUILD_ALL_SERIES:
        return True
    if agency is AgencyId.ESTAT:
        return _macro_relevant_estat(record)
    if total_flows <= _SMALL_AGENCY_THRESHOLD:
        return True
    # Default for other large agencies: macro keyword match on title/id.
    blob = f"{record.dataset_id} {record.title}"
    return _MACRO_TITLE_RE.search(blob) is not None


def select_series_records(
    agency: AgencyId,
    records: list[DatasetRecord],
) -> list[DatasetRecord]:
    """Filter *records* to flows selected for series catalog builds."""

    candidates = [r for r in records if not _is_derived(r)]
    total = len(candidates)
    return [r for r in candidates if should_build_series_catalog(agency, r, total_flows=total)]


__all__ = [
    "select_series_records",
    "should_build_series_catalog",
]
