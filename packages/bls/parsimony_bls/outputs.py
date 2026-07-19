"""Output schemas for the BLS connectors."""

from __future__ import annotations

from parsimony.catalog.search import RANKING_COLUMNS
from parsimony.result import Column, ColumnRole, OutputSpec

from parsimony_bls.surveys import SURVEYS_NAMESPACE, series_namespace

# --- fetch -----------------------------------------------------------------

BLS_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="bls"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

# --- tier-1 surveys enumerator ---------------------------------------------

BLS_SURVEYS_ENUM_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="code",
            role=ColumnRole.KEY,
            namespace=SURVEYS_NAMESPACE,
            description="BLS survey abbreviation (e.g. 'CU').",
        ),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="survey", role=ColumnRole.METADATA),
        Column(name="has_series_catalog", role=ColumnRole.METADATA),
    ]
)

# --- tier-2 per-survey series enumerator (dynamic schema) ------------------

#: Declared schema for the ``@connector`` decorator. The per-survey dimension
#: columns are dynamic, so a ``*`` METADATA wildcard passes them through; the
#: catalog builder re-projects with the per-survey namespace stamped.
BLS_SERIES_ENUM_OUTPUT = OutputSpec(
    columns=[
        Column(
            name="code",
            role=ColumnRole.KEY,
            description="BLS series id (composed from the survey's dimension codes).",
        ),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="*", role=ColumnRole.METADATA),
    ]
)


def series_enum_output(survey: str) -> OutputSpec:
    """Per-call series-enumeration schema with the per-survey KEY namespace stamped."""
    return OutputSpec(
        columns=[
            Column(name="code", role=ColumnRole.KEY, namespace=series_namespace(survey)),
            Column(name="title", role=ColumnRole.TITLE),
            Column(name="*", role=ColumnRole.METADATA),
        ]
    )


# --- search ----------------------------------------------------------------

BLS_SURVEYS_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace=SURVEYS_NAMESPACE),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="survey", role=ColumnRole.METADATA),
        Column(
            name="dimensions",
            role=ColumnRole.METADATA,
            description="Dimension manifest (codes + labels) for series-id construction.",
        ),
        *RANKING_COLUMNS,
    ]
)

BLS_SERIES_SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="bls"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="survey", role=ColumnRole.METADATA),
        Column(name="namespace", role=ColumnRole.METADATA),
        *RANKING_COLUMNS,
    ]
)


__all__ = [
    "BLS_FETCH_OUTPUT",
    "BLS_SERIES_ENUM_OUTPUT",
    "BLS_SERIES_SEARCH_OUTPUT",
    "BLS_SURVEYS_ENUM_OUTPUT",
    "BLS_SURVEYS_SEARCH_OUTPUT",
    "series_enum_output",
]
