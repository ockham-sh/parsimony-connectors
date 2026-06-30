"""World Bank connector output schemas."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

# ``worldbank_fetch`` returns one row per (country, indicator, year).
# KEY is ``indicator_id`` (the WB series code) so the result can be routed
# back to a follow-up fetch or search call. ``country`` rides as metadata
# alongside ``indicator_name``.
WORLDBANK_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="indicator_id", role=ColumnRole.KEY, namespace="worldbank"),
        Column(name="indicator_name", role=ColumnRole.TITLE),
        Column(name="country", role=ColumnRole.METADATA),
        Column(name="country_iso3", role=ColumnRole.METADATA),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

FETCH_COLUMNS: tuple[str, ...] = (
    "indicator_id",
    "indicator_name",
    "country",
    "country_iso3",
    "date",
    "value",
)

# ``worldbank_search`` returns one row per matching indicator.
WORLDBANK_SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="indicator_id", role=ColumnRole.KEY, namespace="worldbank"),
        Column(name="indicator_name", role=ColumnRole.TITLE),
        Column(name="source_note", role=ColumnRole.METADATA),
        Column(name="source_org", role=ColumnRole.METADATA),
        Column(name="page", role=ColumnRole.METADATA),
    ]
)

SEARCH_COLUMNS: tuple[str, ...] = (
    "indicator_id",
    "indicator_name",
    "source_note",
    "source_org",
    "page",
)
