"""BLS survey identity: namespaces, the headline allowlist, and id helpers.

A *survey* is BLS's top-level program (e.g. ``CU`` = CPI All-Urban, ``CE`` =
Current Employment Statistics). It is the analogue of an SDMX *dataflow*: it owns
a set of dimension code tables and a series-id grammar. Tier-1 discovery is over
surveys; tier-2 is per-survey series.
"""

from __future__ import annotations

from parsimony.errors import InvalidParameterError

#: Tier-1 (surveys) catalog namespace.
SURVEYS_NAMESPACE = "bls_surveys"

#: Tier-2 per-survey series catalogs are pre-published / lazy-built only for these
#: headline economic surveys. Everything else stays reachable by id construction
#: (tier-1 dimension manifest) + ``bls_fetch``; its ``.series`` file is too large
#: to index sanely (the GB-scale microdata tail). Extend as coverage grows.
HEADLINE_SURVEYS: frozenset[str] = frozenset(
    {
        "CU",  # CPI — All Urban Consumers
        "CW",  # CPI — Urban Wage Earners
        "SU",  # Chained CPI
        "AP",  # CPI — Average Price Data
        "WP",  # PPI — Commodities
        "PC",  # PPI — Industry
        "ND",  # PPI — Industry (new)
        "EI",  # Import/Export Price Indexes
        "CE",  # Employment, Hours & Earnings (National, CES)
        "SM",  # State & Area Employment (CES)
        "LN",  # Labor Force Statistics (CPS)
        "LA",  # Local Area Unemployment Statistics
        "JT",  # JOLTS
        "BD",  # Business Employment Dynamics
        "CI",  # Employment Cost Index
        "EC",  # Employment Cost Index
        "CC",  # Employer Costs for Employee Compensation
        "PR",  # Major Sector Productivity & Costs
        "MP",  # Major Sector Total Factor Productivity
        "IP",  # Industry Productivity
    }
)

#: Surveys whose ``.series`` flat file does NOT carry a ready-made ``series_title``
#: column — their titles are composed from the dimension code tables. Best-effort:
#: title composition falls back gracefully for any survey not listed here too.
TITLELESS_SURVEYS: frozenset[str] = frozenset({"SM", "JT", "PR"})


def normalize_survey(survey: str) -> str:
    """Validate and upper-case a survey abbreviation (e.g. ``" cu "`` → ``"CU"``)."""
    raw = (survey or "").strip().upper()
    if not (2 <= len(raw) <= 4) or not raw.isalnum():
        raise InvalidParameterError("bls", f"survey must be a 2-4 char BLS abbreviation (e.g. 'CU'); got {survey!r}")
    return raw


def series_namespace(survey: str) -> str:
    """Per-survey series catalog namespace: ``CU`` → ``bls_series_cu``."""
    return f"bls_series_{normalize_survey(survey).lower()}"


def parse_series_namespace(namespace: str) -> str:
    """Map ``bls_series_cu`` back to the survey abbreviation ``CU``."""
    prefix = "bls_series_"
    if not namespace.startswith(prefix) or len(namespace) <= len(prefix):
        raise ValueError(f"Not a BLS series namespace: {namespace!r}")
    return namespace.removeprefix(prefix).upper()


def is_headline(survey: str) -> bool:
    """Whether *survey* has a pre-published / lazy-buildable series catalog."""
    return normalize_survey(survey) in HEADLINE_SURVEYS


__all__ = [
    "HEADLINE_SURVEYS",
    "SURVEYS_NAMESPACE",
    "TITLELESS_SURVEYS",
    "is_headline",
    "normalize_survey",
    "parse_series_namespace",
    "series_namespace",
]
