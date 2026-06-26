"""EIA connector output schemas.

The catalog row is one per **leaf dataset** (the route-tree terminal node). Its
metadata carries the dataset's *query vocabulary* — the measures it accepts as
``data[0]=`` and the facet dimensions it accepts as ``facets[...]`` filters — so
an agent can read a search hit and construct a precise ``eia_fetch`` /
``eia_facets`` call without a blind round-trip (the SDMX/BLS dimension-manifest
pattern). The ``code`` KEY holds the route path (e.g. ``petroleum/pri/spt``).
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

EIA_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="measures", role=ColumnRole.METADATA),
        Column(name="facets", role=ColumnRole.METADATA),
        Column(name="frequencies", role=ColumnRole.METADATA),
        Column(name="default_frequency", role=ColumnRole.METADATA),
        Column(name="start", role=ColumnRole.METADATA),
        Column(name="end", role=ColumnRole.METADATA),
        Column(name="units", role=ColumnRole.METADATA),
    ]
)

ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "category",
    "measures",
    "facets",
    "frequencies",
    "default_frequency",
    "start",
    "end",
    "units",
)

# Fetch returns long-format observations. ``route`` is the KEY (param_key=route
# links a catalog hit back to the fetch parameter); the selected measure is
# normalized to ``value``; every other EIA column (facet codes + their ``-name``
# labels, ``series``, ``series-description``, ``units``) folds in as DATA so a
# multi-series fetch stays disambiguated.
EIA_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="route", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="period", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

# Fetch by legacy APIv1 series id (the out-of-tree `/v2/seriesid/{id}` path —
# e.g. `PET.RWTC.D`, `ELEC.SALES.CO-RES.A`). Same observation shape as the route
# fetch; the series id fully specifies the measure, which is normalized to value.
EIA_SERIES_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="eia"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="period", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

# Facet-value discovery: one row per valid value of a dataset's facet dimension,
# so an agent can narrow a fetch to a specific series (essential on huge datasets
# — electricity hourly is ~18M rows). KEY=facet_value, TITLE=name.
EIA_FACETS_OUTPUT = OutputConfig(
    columns=[
        Column(name="facet_value", role=ColumnRole.KEY, namespace="eia"),
        Column(name="name", role=ColumnRole.TITLE),
        Column(name="facet", role=ColumnRole.METADATA),
        Column(name="route", role=ColumnRole.METADATA),
    ]
)
