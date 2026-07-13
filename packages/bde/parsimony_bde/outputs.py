"""BdE connector output schemas."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

BDE_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="alias", role=ColumnRole.METADATA),
        Column(name="dataset", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="decimals", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
        Column(name="n_obs", role=ColumnRole.METADATA),
        Column(name="source_org", role=ColumnRole.METADATA),
    ]
)

BDE_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

ENUMERATE_COLUMNS: tuple[str, ...] = (
    "key",
    "title",
    "description",
    "source",
    "alias",
    "dataset",
    "category",
    "frequency",
    "unit",
    "decimals",
    "start_date",
    "end_date",
    "n_obs",
    "source_org",
)
