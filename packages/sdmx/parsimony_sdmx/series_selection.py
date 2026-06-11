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
    r"gdp|gross value|value added|gva|inflation|cpi|hicp|price\w*|employment|unemployment|wage|interest|yield|"
    r"exchange|fx|money|credit|loan|debt|fiscal|budget|trade|balance|payment|"
    r"current account|financial|monetary|bank|reserve|deposit|bond|security|"
    r"rate\w*|index|production|industrial|retail|confidence|pmi|forecast|"
    r"national account|government|tax|revenue|expenditure|import\w*|export\w*|"
    r"labour|labor|housing|mortgage|equity|stock|market|commodity|energy|"
    r"macro|economic|finance|financial|indicator|survey|turnover|investment|"
    r"capital formation|sentiment|productivity|iip|fdi"
    r")\b",
    re.IGNORECASE,
)

_NON_MACRO_TITLE_RE = re.compile(
    r"\b("
    r"regional|municipal|agriculture|forestry|fishery|fishing|livestock|"
    r"tourism|hotel|restaurant|culture|sport|environment|waste|water|air|"
    r"health care|hospital|education|school|research|patent|"
    r"transport equipment|vehicle registration|road accident|"
    r"energy balance|electricity generation by fuel|migration|time[- ]use"
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


# High-value ESTAT macro families to prebuild first when a build is time-boxed.
# Everything not prebuilt still lazy-builds on first ``sdmx_series_search``.
_ESTAT_PRIORITY_PREFIXES: tuple[str, ...] = (
    "NAMA_",  # annual national accounts
    "NAMQ_",  # quarterly national accounts
    "NASA_",  # sector accounts
    "NASQ_",
    "GOV_",  # government finance / debt
    "PRC_HICP",  # harmonised inflation
    "PRC_PPI",  # producer prices
    "STS_",  # short-term business statistics
    "IRT_",  # interest rates
    "EI_",  # economic indicators (BCS etc.)
    "BOP_",  # balance of payments
    "BPM6",
    "EXT_",  # external trade
    "LFSI_",  # labour force main indicators
    "LFSA_",
    "UNE_",  # unemployment
    "EI_LMHR",
    "TIPS",  # principal European economic indicators
    "TEINA",  # short-term indicators (Eurostat "tein*" tables)
    "TIPSGO",
)


def _estat_priority_rank(record: DatasetRecord) -> int:
    upper = record.dataset_id.upper()
    for rank, prefix in enumerate(_ESTAT_PRIORITY_PREFIXES):
        if upper.startswith(prefix):
            return rank
    return len(_ESTAT_PRIORITY_PREFIXES)


def prioritize_series_records(
    agency: AgencyId,
    records: list[DatasetRecord],
) -> list[DatasetRecord]:
    """Stable-sort *records* so the highest-value flows build first.

    Only ESTAT has a meaningful priority order (its selection is huge and
    time-boxed builds want the macro core first). Other agencies build their
    full bounded selection, so order is preserved.
    """

    if agency is not AgencyId.ESTAT:
        return list(records)
    return sorted(records, key=_estat_priority_rank)


__all__ = [
    "prioritize_series_records",
    "select_series_records",
    "should_build_series_catalog",
]
