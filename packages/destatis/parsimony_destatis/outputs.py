"""Destatis connector output schemas."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

DESTATIS_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),
        Column(name="parent_statistic", role=ColumnRole.METADATA),
        Column(name="subject_area", role=ColumnRole.METADATA),
        Column(name="title_de", role=ColumnRole.METADATA),
        Column(name="title_en", role=ColumnRole.METADATA),
        Column(name="variable_codes", role=ColumnRole.METADATA),
        Column(name="variable_names_en", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),
    ]
)

DESTATIS_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="destatis"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "entity_type",
    "parent_statistic",
    "subject_area",
    "title_de",
    "title_en",
    "variable_codes",
    "variable_names_en",
    "source",
)
